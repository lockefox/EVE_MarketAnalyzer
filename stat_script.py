#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime
import numpy
import rpy2.robjects as robjects
import rpy2
from rpy2.robjects.packages import importr

import pandas as pd
import pandas.io.sql as psql

from scipy.stats import norm

from ema_config import *

global V

localdir = "C:\Users\John\Dropbox\EMA"

basedir = localdir if os.path.exists(localdir) else ""

class AttrLogger(object):
	def __init__(self):
		super(AttrLogger, self).__setattr__('start_time', time.clock())
	def __setattr__(self, name, value):
		print "{0:6.1f} Finished calculating {1}.".format(time.clock()-self.start_time, name)
		super(AttrLogger, self).__setattr__(name, value)

V = AttrLogger()

R_configured = False
img_type = conf.get('STATS','format')
img_X = conf.get('STATS','plot_width')
img_Y = conf.get('STATS','plot_height')

default_TA = conf.get('STATS','default_quantmod')
default_subset = conf.get('STATS','default_subset')

today = datetime.now().strftime('%Y-%m-%d')

convert = {}
desired_stats = ['volume','price_delta_sma','price_delta_sma']
global_debug = int(conf.get('STATS','debug'))

data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()

def fetch_market_data(days=366, window=15, region=10000002, debug=global_debug):
	global V, desired_stats
	print 'Fetching market data ...'
	raw_query = \
		'''SELECT itemid, price_date, volume, avgprice
		   FROM crest_markethistory
		   WHERE regionid = %s
		   AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
		   ORDER BY itemid, price_date''' % (region, days+30)
	if debug:
		raw_query = \
		'''SELECT itemid, price_date, volume, avgprice
		   FROM crest_markethistory
		   WHERE regionid = %s
		   AND itemid = 34
		   AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
		   ORDER BY itemid, price_date''' % (region, days+30)

	V.raw_query = raw_query
	raw_data = psql.read_sql(raw_query, data_conn, parse_dates=['price_date'])
	V.raw_data = raw_data
	expected_dates = pd.DataFrame(raw_data[raw_data.itemid == 34].price_date)
	expected_dates.index = expected_dates.price_date
	V.expected_dates = expected_dates
	raw_data_filled = pd.ordered_merge(
		raw_data[raw_data.itemid.isin(convert.index)], 
		expected_dates,
		on='price_date',
		left_by='itemid'
		)

	raw_data_filled['present'] = raw_data_filled.avgprice / raw_data_filled.avgprice

	raw_data_filled.fillna({'volume':0}, inplace=True)

	raw_data_filled['price_delta_sma'] = \
		raw_data_filled \
		.groupby('itemid') \
		.avgprice \
		.apply(
			lambda x: x - pd.rolling_mean(
				x.interpolate()
				 .fillna(method='bfill'),
				window
				)
			)

	# raw_data_filled['price_delta_sma2'] = raw_data_filled['price_delta_sma'] ** 2

	raw_data_filled['price_delta_smm'] = \
		raw_data_filled \
		.groupby('itemid') \
		.avgprice \
		.apply(
			lambda x: x - pd.rolling_median(
				x.interpolate()
				 .fillna(method='bfill'),
				window
				)
			)

	V.raw_data_filled = raw_data_filled
	# raw_data_filled['price_delta_smm2'] = raw_data_filled['price_delta_smm'] ** 2 
	desired_stats = ['volume','price_delta_sma','price_delta_smm']
	raw_data_filled.index = raw_data_filled.itemid
	raw_data_filled = \
		raw_data_filled \
		.groupby('itemid') \
		.filter(lambda x: len(x.index) >= window) \
	    .groupby('itemid') \
		.tail(days)
	return raw_data_filled.groupby('itemid')
	
def crunch_market_stats(data_groups, report_sigmas, filter_sigmas, region=10000002, debug=global_debug):
	global V, desired_stats

	desired = desired_stats

	print 'Crunching Stats'
	
	def p(x, s): 
		c = x.dropna()
		ct = c.count()
		pctile = norm.cdf(-abs(s))
		return numpy.percentile(c, norm.cdf(s)*100) if ct >= 1/pctile else numpy.NaN

	def new_func(name, lam, l=locals()):
		lam.func_name = name
		l[name] = lam
		return lam

	MIN = new_func('MIN', lambda x: x.min())
	P10 = new_func('P10', lambda x: numpy.percentile(x, 10))
	MED = new_func('MED', lambda x: x.median())
	AVG = new_func('AVG', lambda x: x.mean())
	P90 = new_func('P90', lambda x: numpy.percentile(x, 90))
	MAX = new_func('MAX', lambda x: x.max())
	STD = new_func('STD', lambda x: x.std())

	standard_stats = [MIN, MED, AVG, MAX]
	sigma_stats = filter_sigmas

	if debug:
		standard_stats = [MIN, P10, MED, AVG, P90, MAX, STD]
		sigma_stats = report_sigmas

	V.stats = stats = data_groups[desired].agg(standard_stats)
	stats[('all','count')] = data_groups['present'].count()
	V.qs = qs = data_groups[desired].quantile([norm.cdf(sig) for sig in sigma_stats])
	qs.index.names = ['itemid','sigma']
	qs.index.set_levels([str(s) for s in sigma_stats], level=1, inplace=True)
	stats = pd.merge(stats, qs.unstack(), left_index=True, right_index=True, copy=False)

	V.stats = stats
	return stats

def sig_int_to_str(sigma_num):
	return "S{:.1f}".format(sigma_num).replace("-","N").replace(".","P")

def cmp_sigs(x, y):
	x_val, x_ct = x
	y_val, y_ct = y
	if x_ct == y_ct:
		if abs(y_val) == abs(x_val):
			return 1 if x_val < y_val else -1
		else:
			return 1 if abs(x_val) < abs(y_val) else -1
	else:
		return 1 if x_ct < y_ct else -1

def flag_volume(v, vol_floor, stats):
	itemid = v.index[0]
	if v[v == 0].any() or v.isnull().any(): return numpy.nan
	m = v.mean()
	if m < vol_floor: return numpy.nan
	cuts = cuts_from_stats(stats, itemid, 'volume')
	if not cuts: return numpy.nan
	val = pd.cut([m], **cuts)[0]
	return "vol{: >4.1f}".format(val) if val <> 0.0 else numpy.nan

def label_from_flags(flags):
	vcl = list(flags[flags<>0.0].value_counts().iteritems())
	vcl.sort(cmp_sigs)
	return vcl[0][0]

def flag_price(p, price_stat, stats, min_flags=5):
	itemid = p.index[0]
	if p.isnull().any(): 
		return numpy.nan
	cuts = cuts_from_stats(stats, itemid, price_stat)
	if not cuts: 
		return numpy.nan
	flags = pd.cut(p, **cuts)
	if flags[flags <> 0].count() >= min_flags:
		return label_from_flags(flags)
	else:
		return numpy.nan

def price_pair_to_group(r):
	i, smm, sma = r
	smm_ = "{: >4.1f}".format(smm) if not numpy.isnan(smm) else "____"
	sma_ = "{: >4.1f}".format(sma) if not numpy.isnan(sma) else "____"
	return "Psmm{0}=sma{1}".format(smm_,sma_)

def sigma_report(
		stats, 
		data_groups, 
		days, 
		vol_floor = 100, 
		region=10000002,
		debug=global_debug
		):
	global V

	def flag_vol(v): return flag_volume(v, vol_floor, stats)
	def flag_smm(p): return flag_price(p, 'price_delta_smm', stats)
	def flag_sma(p): return flag_price(p, 'price_delta_sma', stats) 

	flags_wanted = {
		'volume': flag_vol,
		'price_delta_smm': flag_smm,
		'price_delta_sma': flag_sma
		}
	V.flags_wanted = flags_wanted
	flagged = \
		data_groups \
		.tail(days) \
		.groupby('itemid') \
	    .filter(lambda x: x.volume.mean() >= 5) \
		.groupby('itemid') \
		.agg(flags_wanted) \
		.dropna(how='all') \
		.reset_index()
	flagged.index = flagged.itemid
	V.flagged = flagged
	V.vol_flagged = vol_flagged = flagged[['itemid','volume']].dropna().groupby('volume').groups
	price_flags = \
		flagged[flagged.volume.isnull()][['itemid','price_delta_smm','price_delta_sma']] \
		.dropna(how='all', subset=['price_delta_smm','price_delta_sma'])
	price_flags['price'] = price_flags.apply(price_pair_to_group, axis=1)
	V.price_flagged = price_flagged = price_flags[['price','itemid']].groupby('price').groups

	return vol_flagged, price_flagged

def fetch_and_plot(data_struct, which_flags, TA_args = "", region=10000002):
	global R_configured, basedir
	if not R_configured:
		print 'Setting up R libraries'
		importr('jsonlite')
		importr('quantmod',robject_translations = {'skeleton.TA':'skeletonTA'})
		importr('data.table')
		R_configured = True

	print 'setting up dump path'
	out_dir = os.path.join(basedir, 'plots', today, which_flags)
	if not os.path.exists(out_dir):
		os.makedirs(out_dir)
	
	for group, item_list in data_struct.iteritems():
		dump_path = os.path.join(basedir, 'plots', today, which_flags, group).replace("\\","/")
		print dump_path
		if not os.path.exists(dump_path):
			os.makedirs(dump_path)
			
		for itemid in item_list:
			query_str = '%smarket/%s/types/%s/history/' % (crest_path, region, itemid)
			item_name = convert.at[itemid,'name']
			if itemid == 29668:
				item_name = 'PLEX'	#Hard override, because PLEX name is dumb
			item_name = sanitize(item_name)	#remove special chars
			img_path = '{dump_path}/{item_name}_{region}_{today}.{img_type}'.format(
				dump_path=dump_path,
				region=region,
				item_name=item_name,
				today=today,
				img_type=img_type
				)
			plot_title = '%s %s' % (item_name, today)
			print '\tplotting %s' % item_name
			R_command_parametrized = '''
				market.json <- fromJSON(readLines('{query_str}'))
				market.data <- data.table(market.json$items)
				market.data <- market.data[,list(Date = as.Date(date),
												Volume= volume,
												High  = highPrice,
												Low   = lowPrice,
												Close =avgPrice[-1],
												Open  = avgPrice)]
				n <- nrow(market.data)
				market.data <- market.data[1:n-1,]
				market.data.ts <- xts(market.data[,-1,with=F],order.by=market.data[,Date],period=7)
				{img_type}('{img_path}',width = {img_X}, height = {img_Y})
				chartSeries(market.data.ts,
							name = '{plot_title}',
							TA = '{default_TA}{TA_args}',
							subset = '{default_subset}')
				dev.off()'''
			R_command = R_command_parametrized.format(
				query_str=query_str,
				img_type=img_type,
				img_path=img_path,
				img_X=img_X,
				img_Y=img_Y,
				plot_title=plot_title,
				default_TA=default_TA,
				TA_args=TA_args,
				default_subset=default_subset
				)


			#robjects.r(R_command)	
			#print R_command
			for tries in range (0,retry_limit):
				try:
					robjects.r(R_command)
				except rpy2.rinterface.RRuntimeError as e:
					print '\t\tFailed pull %s' % e
					continue
				break
			else:
				print '\t\tskipping %s' % item_name
				continue

def main(region=10000002):
	report_sigmas = [
		-2.5,
		-2.0,
		-1.5,
		-1.0,
		-0.5,
		 # 0.0,
		 0.5,
		 1.0,
		 1.5,
		 2.0,
		 2.5
	]
	
	filter_sigmas = [
		-2.5,
		-2.0,
		-1.5,
		#-1.0,
		 1.5,
		 2.0,
		 2.5
	]
	global convert
	R_config_file = open(conf.get('STATS','R_config_file'),'r')
	R_todo = json.load(R_config_file)
	
	print 'Fetching item list from SDE: %s' % sde_schema

	convert = psql.read_sql(
		'''SELECT typeid as itemid, typename as name
		   FROM invtypes conv
		   JOIN invgroups grp ON (conv.groupID = grp.groupID)
		   WHERE marketgroupid IS NOT NULL
		   AND conv.published = 1
		   AND grp.categoryid NOT IN (9,16,350001,2)
		   AND grp.groupid NOT IN (30,659,485,485,873,883)
		   ORDER BY itemid''', 
		sde_conn, 
		index_col=['itemid']
		)
	V.convert = convert
	
	#print 'Plotting Forced Group'
	#R_config_file = open(conf.get('STATS','R_config_file'),'r')
	#R_todo = json.load(R_config_file)
	#fetch_and_plot(R_todo['forced_plots'], 'forced', ";addRSI();addLines(h=30, on=4);addLines(h=70, on=4)",region=region)
	#sys.exit()
	
	market_data_groups = fetch_market_data(region=region)
	V.market_data_groups = market_data_groups
	market_sigmas = crunch_market_stats(market_data_groups, report_sigmas, filter_sigmas, region=region)
	V.market_sigmas = market_sigmas
	data_groups = market_data_groups
	vol_flagged, price_flagged = \
		sigma_report(
			market_sigmas, 
			market_data_groups,  
			10, 
			region=region
			)
	return
	print "write flagged groups"
	outfile = open(os.path.join(basedir,'sig_flags.txt'),'w')
	for sig_level,itemids in itertools.chain(vol_flagged.iteritems(), price_flagged.iteritems()):
		outfile.write('%s FLAGS\n' % sig_level)
		for item in itemids:
			itemname=''
			try:
				itemname = convert.at[item,'name']
			except KeyError as e:
				pass
			outfile.write('\t%s,%s\n' % (item,itemname))
		
	outfile.close()
	print 'Plotting price test'
	s2p5 = {"Psmm-2.5=sma-2.5": price_flagged["Psmm-2.5=sma-2.5"]}
	fetch_and_plot(s2p5, 'price-test')

	print 'Plotting  Group'
	fetch_and_plot(price_flagged, 'price', region=region)
	fetch_and_plot(vol_flagged, 'volume', region=region)
	
	print 'Plotting Forced Group'
	R_config_file = open(conf.get('STATS','R_config_file'),'r')
	R_todo = json.load(R_config_file)
	fetch_and_plot(R_todo['forced_plots'], 'forced', ";addRSI();addLines(h=30, on=4);addLines(h=70, on=4)",region=region)	

def cuts_from_stats(stats, itemid, category):
	s = stats.loc[itemid, category]
	r = s.loc['-2.5':'2.5']
	if r.isnull().any(): return {}
	can_flag = abs(norm.ppf(1.0/stats.loc[itemid, ('all','count')]))
	labels = [float(l) for l in r.index.tolist()]
	if 0.0 in labels: raise Exception('0.0  disallowed in flagging sigmas!!')
	prev_l = -numpy.inf
	z_loc = -1
	for z, l in enumerate(labels):
		if prev_l < 0 and l > 0:
			z_loc = z
			break
	if z_loc >= 0:
		labels.insert(z_loc, 0.0)
	else:
		labels.append(0.0)
	values = r.values.tolist()
	values.append(numpy.inf)
	prev_v = -numpy.inf
	fixed_vals = [prev_v]
	fixed_labels = []
	for l, v in zip(labels, values):
		if abs(l) > can_flag or v <= prev_v:
			prev_v = v
			continue
		fixed_vals.append(v)
		fixed_labels.append(l)
		prev_v = v
	return {'bins': fixed_vals, 'labels': fixed_labels} if fixed_labels else {}

def plot_flag(itemid, data_groups, stats, desired, style=('go','ro'), range=slice(None,None)):
	cuts = cuts_from_stats(stats, itemid, desired)
	g = data_groups.get_group(itemid)
	g.index = g.price_date
	fname = desired + "_flag"
	g[fname] = pd.cut(g[desired], **cuts)
	ax = g.avgprice[range].plot()
	if not g[g[fname]<0.0].avgprice[range].empty:
		ax = g[g[fname]<0.0].avgprice[range].plot(style=style[0], markersize=10, alpha=0.5, ax=ax)
	if not g[g[fname]>0.0].avgprice[range].empty:
		ax = g[g[fname]>0.0].avgprice[range].plot(style=style[1], markersize=10, alpha=0.5, ax=ax)
	return ax

def plot_flags(
		itemid, 
		data_groups, 
		stats, 
		desired=['price_delta_sma', 'price_delta_smm', 'volume'], 
		range=slice(None,None),
		style=itertools.repeat(('o','o'))
		):
	return [
		plot_flag(itemid, data_groups, stats, d, style=s, range=range) 
			for (d, s) in zip(desired, style)
		]

def hist_compare(itemid, desired=['price_delta_smm','price_delta_sma']): 
	return (
		V.market_data_groups
		 .get_group(itemid)
		 .loc[:,desired]
		 .plot(kind='hist',alpha=0.5,bins=50)
	)

if __name__ == "__main__":
	main()
	
	#for region in trunc_region_list.iterkeys():
	#	print "Generating plots for {region}".format(region=region)
	#	main(region=region) 

# 30633 = wrecked weapon subroutines
# 12801 = Javelin M

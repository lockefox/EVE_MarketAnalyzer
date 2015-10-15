DROP TABLE IF EXISTS `ema_reprocess`;
CREATE TABLE `ema_reprocess` (
	`value_date`	     DATE NOT NULL,
	`itemid`	     INT(8) NOT NULL,
	`regionid`           INT(8) NOT NULL,
	`avgValue`	     FLOAT(13,2) NULL,
	PRIMARY KEY (value_date, itemid,regionid))
ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX value_dates ON ema_process(value_date);
CREATE INDEX reprocess_itemids ON ema_process(itemid);
CREATE INDEX reprocess_regionids ON ema_process(regionid);

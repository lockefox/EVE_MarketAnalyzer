from exceptions import Exception

class QueryException(Exception):
	def __init__(self, msg="Useless generic Exception"):
		Exception.__init__(self, msg)

class TooFewRequiredParameters(QueryException):
	def __init__(self, query, required):
		req_list = '\n\t'.join(required)
		QueryException.__init__(self, "Query %s must include one of:\n\t%s" % (query, req_list))

class InvalidQueryParameter(QueryException):
	def __init__(self, param, valid_params):
		param_list = '\n\t'.join(valid_params)
		QueryException.__init__(self, "'%s'. Valid parameters:\n\t%s" % (param, param_list))

class TooManyIDsRequested(QueryException):
	def __init__(self, modifier, limit):
		QueryException.__init__(self, "'%s'. Query limit: %s" % (modifier, limit))

class InvalidQueryValue(QueryException):
	def __init__(self, value, msg):
		QueryException.__init__(self, "'%s'. %s" % (value, msg))

class InvalidDateFormat(QueryException):
	def __init__(self, datevalue, formats):
		format_list = '\n\t'.join(formats)
		Exception.__init__(self, "'%s'.  Valid formats:\n\t%s" % (datevalue, format_list))

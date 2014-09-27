DROP TABLE IF EXISTS `crest_serverprices`;
CREATE TABLE `crest_serverprices` (
	`price_date`		DATE NOT NULL,
	`typeid`			INT(8) NOT NULL,
	`adjustedprice`		FLOAT(12,2) NULL DEFAULT 0.0,
	`averageprice`		FLOAT(12,2) NULL DEFAULT 0.0,
	PRIMARY KEY (price_date,typeid))
ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX server_pricedate ON crest_serverprices(price_date);
CREATE INDEX server_typeids   ON crest_serverprices(typeid)
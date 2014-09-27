DROP TABLE IF EXISTS `crest_systemindex`;
CREATE TABLE `crest_systemindex`(
	`solarsystemid`		INT(8)	NOT NULL,
	`activityid`		INT(2)	NOT NULL,
	`activity_date`		DATE	NOT NULL,
	`costindex`			FLOAT(20,20) NULL,
	PRIMARY KEY (solarsystemid, activityid, activity_date))
ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE INDEX solarsystem ON crest_systemindex(solarsystemid);
CREATE INDEX activity ON crest_systemindex(solarsystemid);
CREATE INDEX entrydate ON crest_systemindex(activity_date);
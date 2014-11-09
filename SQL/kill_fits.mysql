DROP TABLE IF EXISTS `kill_fits`;
CREATE TABLE `kill_fits`(
	`killID` int(32) NOT NULL,
	`characterID` int(16) NOT NULL,
	`corporationID` int(16) NOT NULL,
	`allianceID` int(16) DEFAULT NULL,
	`factionID` int(16) DEFAULT NULL,
	`shipTypeID` int(8) NOT NULL,
	`typeID` int(8) NOT NULL,
	`flag` int(8) NOT NULL,
	`qtyDropped` int(32) NOT NULL DEFAULT 0,
	`qtyDestroyed` int(32) NOT NULL DEFAULT 0,
	`singleton` tinyint(1) NOT NULL DEFAULT 0,
	PRIMARY KEY (killID,typeID,flag))
ENGINE=InnoDB DEFAULT CHARSET=latin1
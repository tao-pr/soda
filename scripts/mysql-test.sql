USE test;
DROP TABLE IF EXISTS `foo`;

CREATE TABLE `foo` (
  `uuid` varchar(36) NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `code` int unsigned DEFAULT 0,
  `baz` double,

  PRIMARY KEY (`uuid`)
);


--
-- Dumping data for table
--

LOCK TABLES `foo` WRITE;

INSERT INTO `foo` VALUES ('9a02de5b-9449-466d-9269-ba798e7b56dd', 'Melon', 15, 2.55551);

UNLOCK TABLES;
/*!40101 SET NAMES utf8mb4*/;
CREATE TABLE `odd` (
  `Key` int NOT NULL,
  `my col` varchar(16) DEFAULT NULL,
  `select` int DEFAULT NULL,
  `f` float DEFAULT NULL,
  `zd` date NOT NULL,
  `tu` tinyint unsigned DEFAULT NULL,
  `mi` mediumint DEFAULT NULL,
  `bi` bigint DEFAULT NULL,
  `bits` bit(8) DEFAULT NULL,
  `wide` decimal(40,2) DEFAULT NULL,
  `bin` varbinary(8) DEFAULT NULL,
  `cs` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  PRIMARY KEY (`Key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

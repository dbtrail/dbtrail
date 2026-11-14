/*!40101 SET NAMES utf8mb4*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `items` (
  `id` int NOT NULL,
  `name` varchar(64) DEFAULT NULL,
  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `price` decimal(10,2) DEFAULT NULL,
  `qty` int unsigned DEFAULT NULL,
  `big` bigint unsigned DEFAULT NULL,
  `flag` tinyint(1) DEFAULT NULL,
  `small` smallint DEFAULT NULL,
  `ratio` double DEFAULT NULL,
  `born` date DEFAULT NULL,
  `seen` datetime(6) DEFAULT NULL,
  `stamp` timestamp NULL DEFAULT NULL,
  `tod` time DEFAULT NULL,
  `yr` year DEFAULT NULL,
  `kind` enum('a','b','c') DEFAULT NULL,
  `tags` set('x','y','z') DEFAULT NULL,
  `doc` json DEFAULT NULL,
  `raw` varbinary(16) DEFAULT NULL,
  `note` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

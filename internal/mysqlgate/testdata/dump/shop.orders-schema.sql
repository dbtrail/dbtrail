/*!40101 SET NAMES utf8mb4*/;
CREATE TABLE `orders` (
  `id` bigint NOT NULL,
  `item_id` int DEFAULT NULL,
  `amount` decimal(12,4) DEFAULT NULL,
  `status` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

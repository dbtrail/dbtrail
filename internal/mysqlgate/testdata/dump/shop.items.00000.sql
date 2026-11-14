/*!40101 SET NAMES utf8mb4*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40103 SET TIME_ZONE='+00:00' */;
INSERT INTO `items` (`id`,`name`,`code`,`price`,`qty`,`big`,`flag`,`small`,`ratio`,`born`,`seen`,`stamp`,`tod`,`yr`,`kind`,`tags`,`doc`,`raw`,`note`) VALUES(1,"Alice","Ab",10.50,7,18446744073709551615,1,-3,0.5,"2026-01-31","2026-01-31 10:00:00.123456","2026-01-31 10:00:00","10:30:00",2026,"a","x,y",CONVERT("{\"k\": 1, \"s\": \"one\"}" USING UTF8MB4),_binary "AB","Hello there")
,(2,"alice","AB",0.00,0,0,0,0,-1.25,"2024-02-29","2024-02-29 23:59:59.000000","2024-02-29 23:59:59","00:00:00",1999,"b","",CONVERT("[1, 2, 3]" USING UTF8MB4),_binary "ab","hello")
,(3,"BOB","ab",99999999.99,4294967295,9223372036854775808,1,32767,1e10,"1999-12-31","1999-12-31 00:00:00.000001","2001-01-01 00:00:01","838:59:59",2155,"c","x,y,z",CONVERT("null" USING UTF8MB4),_binary "","")
,(4,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
,(5,"Émile","é",-7.25,12,42,0,-32768,3.14159,"2026-12-01","2026-12-01 12:00:00.500000","2026-12-01 12:00:00","-01:00:00",2000,"a","z",CONVERT("{\"k\": 2}" USING UTF8MB4),_binary "Zz","año")
;

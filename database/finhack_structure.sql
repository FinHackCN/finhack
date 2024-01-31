-- MySQL dump 10.13  Distrib 8.0.30, for Linux (x86_64)
--
-- Host: localhost    Database: finhack
-- ------------------------------------------------------
-- Server version	8.0.30

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `auto_train`
--

DROP TABLE IF EXISTS `auto_train`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `auto_train` (
  `id` int NOT NULL AUTO_INCREMENT,
  `start_date` varchar(10) DEFAULT NULL,
  `valid_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `label` varchar(255) DEFAULT NULL,
  `shift` int DEFAULT NULL,
  `param` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `hash` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `loss` varchar(255) DEFAULT NULL,
  `algorithm` varchar(255) DEFAULT NULL,
  `filter` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT '',
  `score` double(10,10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12675 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `backtest`
--

DROP TABLE IF EXISTS `backtest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `backtest` (
  `id` int NOT NULL AUTO_INCREMENT,
  `instance_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `features_list` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `train` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `model` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `strategy` varchar(255) DEFAULT NULL,
  `start_date` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `end_date` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `init_cash` double(100,5) DEFAULT NULL,
  `args` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `history` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `returns` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `logs` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `total_value` double(100,5) DEFAULT NULL,
  `alpha` double(100,5) DEFAULT NULL,
  `beta` double(100,5) DEFAULT NULL,
  `annual_return` double(100,5) DEFAULT NULL,
  `cagr` double(100,5) DEFAULT NULL,
  `annual_volatility` double(100,5) DEFAULT NULL,
  `info_ratio` double(100,5) DEFAULT NULL,
  `downside_risk` double(100,5) DEFAULT NULL,
  `R2` double(100,5) DEFAULT NULL,
  `sharpe` double(100,5) DEFAULT NULL,
  `sortino` double(100,5) DEFAULT NULL,
  `calmar` double(100,5) DEFAULT NULL,
  `omega` double(100,5) DEFAULT NULL,
  `max_down` double(100,5) DEFAULT NULL,
  `SQN` double(100,5) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `filter` varchar(255) DEFAULT '',
  `win` double(100,5) DEFAULT NULL,
  `server` varchar(255) DEFAULT NULL,
  `trade_num` int DEFAULT NULL,
  `runtime` varchar(255) DEFAULT NULL,
  `starttime` varchar(100) DEFAULT NULL,
  `endtime` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `benchReturns` mediumtext,
  `roto` double(100,5) DEFAULT NULL,
  `simulate` int DEFAULT '0',
  `benchmark` varchar(255) DEFAULT NULL,
  `strategy_code` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `instence_id` (`instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=388626 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `factors_analysis`
--

DROP TABLE IF EXISTS `factors_analysis`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `factors_analysis` (
  `id` int NOT NULL AUTO_INCREMENT,
  `factor_name` varchar(255) DEFAULT NULL,
  `days` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `start_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `formula` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `IC` float(10,5) DEFAULT NULL,
  `IR` float(10,5) DEFAULT NULL,
  `IRR` float(10,5) DEFAULT NULL,
  `score` float(10,5) DEFAULT NULL,
  `max_up_corr` float(10,7) DEFAULT NULL,
  `hash` varchar(255) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2388 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `factors_list`
--

DROP TABLE IF EXISTS `factors_list`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `factors_list` (
  `id` int NOT NULL AUTO_INCREMENT,
  `factor_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `indicators` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `func_name` varchar(255) DEFAULT NULL,
  `code` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `return_fileds` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `md5` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `check_type` int DEFAULT '0',
  `status` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'activate',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2406 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `factors_mining`
--

DROP TABLE IF EXISTS `factors_mining`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `factors_mining` (
  `id` int NOT NULL AUTO_INCREMENT,
  `factor_name` varchar(255) DEFAULT NULL,
  `days` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `start_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `formula` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `IC` float(20,5) DEFAULT NULL,
  `IR` float(20,5) DEFAULT NULL,
  `IRR` float(20,5) DEFAULT NULL,
  `score` float(10,5) DEFAULT NULL,
  `hash` varchar(255) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash` (`hash`(32)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=31156 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-01-31 13:01:19

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
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash` (`hash`)
) ENGINE=InnoDB AUTO_INCREMENT=48039 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
  `params` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
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
) ENGINE=InnoDB AUTO_INCREMENT=472599 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
  `source` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `start_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `formula` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `IC` float(10,5) DEFAULT NULL,
  `IR` float(10,5) DEFAULT NULL,
  `Sharpe` float(10,5) DEFAULT NULL,
  `score` float(10,5) DEFAULT NULL,
  `max_up_corr` float(10,7) DEFAULT NULL,
  `hash` varchar(255) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3035 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
) ENGINE=InnoDB AUTO_INCREMENT=2491 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
  `source` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `start_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `formula` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `IC` float(20,5) DEFAULT NULL,
  `IR` float(20,5) DEFAULT NULL,
  `Sharpe` float(20,5) DEFAULT NULL,
  `score` float(10,5) DEFAULT NULL,
  `hash` varchar(255) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash` (`hash`(32)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=33145 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rqalpha`
--

DROP TABLE IF EXISTS `rqalpha`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rqalpha` (
  `instance_id` varchar(64) NOT NULL,
  `strategy_name` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `strategy_file` varchar(255) DEFAULT NULL,
  `run_type` varchar(255) DEFAULT NULL,
  `starting_cash` varchar(255) DEFAULT NULL,
  `STOCK` float DEFAULT NULL,
  `benchmark` varchar(255) DEFAULT NULL,
  `benchmark_symbol` varchar(255) DEFAULT NULL,
  `alpha` float DEFAULT NULL,
  `beta` float DEFAULT NULL,
  `sharpe` float DEFAULT NULL,
  `excess_sharpe` float DEFAULT NULL,
  `information_ratio` float DEFAULT NULL,
  `downside_risk` float DEFAULT NULL,
  `tracking_error` float DEFAULT NULL,
  `sortino` float DEFAULT NULL,
  `volatility` float DEFAULT NULL,
  `excess_volatility` float DEFAULT NULL,
  `max_drawdown` float DEFAULT NULL,
  `excess_max_drawdown` float DEFAULT NULL,
  `excess_returns` float DEFAULT NULL,
  `excess_annual_returns` float DEFAULT NULL,
  `var` float DEFAULT NULL,
  `win_rate` float DEFAULT NULL,
  `excess_win_rate` float DEFAULT NULL,
  `excess_cum_returns` float DEFAULT NULL,
  `profit_loss_rate` float DEFAULT NULL,
  `total_value` float DEFAULT NULL,
  `cash` float DEFAULT NULL,
  `total_returns` float DEFAULT NULL,
  `annualized_returns` float DEFAULT NULL,
  `unit_net_value` float DEFAULT NULL,
  `units` float DEFAULT NULL,
  `benchmark_total_returns` float DEFAULT NULL,
  `benchmark_annualized_returns` float DEFAULT NULL,
  `max_drawdown_duration` text,
  `max_drawdown_duration_start_date` date DEFAULT NULL,
  `max_drawdown_duration_end_date` date DEFAULT NULL,
  `max_drawdown_duration_days` int DEFAULT NULL,
  `turnover` float DEFAULT NULL,
  `excess_max_drawdown_duration` text,
  `excess_max_drawdown_duration_start_date` date DEFAULT NULL,
  `excess_max_drawdown_duration_end_date` date DEFAULT NULL,
  `excess_max_drawdown_duration_days` int DEFAULT NULL,
  `weekly_alpha` float DEFAULT NULL,
  `weekly_beta` float DEFAULT NULL,
  `weekly_sharpe` float DEFAULT NULL,
  `weekly_sortino` float DEFAULT NULL,
  `weekly_information_ratio` float DEFAULT NULL,
  `weekly_tracking_error` float DEFAULT NULL,
  `weekly_max_drawdown` float DEFAULT NULL,
  `weekly_win_rate` float DEFAULT NULL,
  `weekly_volatility` float DEFAULT NULL,
  `weekly_ulcer_index` float DEFAULT NULL,
  `weekly_ulcer_performance_index` float DEFAULT NULL,
  `monthly_sharpe` float DEFAULT NULL,
  `monthly_volatility` float DEFAULT NULL,
  `monthly_excess_win_rate` float DEFAULT NULL,
  `weekly_excess_ulcer_index` float DEFAULT NULL,
  `weekly_excess_ulcer_performance_index` float DEFAULT NULL,
  `avg_daily_turnover` float DEFAULT NULL,
  PRIMARY KEY (`instance_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-01-23 18:27:25

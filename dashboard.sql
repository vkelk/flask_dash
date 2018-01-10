/*
 Source Server Type    : MariaDB
 Source Server Version : 100211
 Source Schema         : dashboard

 Target Server Type    : MariaDB
 Target Server Version : 100211
 File Encoding         : 65001

 Date: 10/01/2018 11:50:59
*/

SET NAMES utf8mb4;
CREATE SCHEMA IF NOT EXISTS `dashboard` DEFAULT CHARACTER SET utf8mb4 ;
USE `dashboard` ;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for accounts
-- ----------------------------
DROP TABLE IF EXISTS `accounts`;
CREATE TABLE `accounts`  (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `email` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `_password` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `authenticated` tinyint(1) NULL DEFAULT 0,
  `email_confirmation_sent_on` datetime(0) NULL DEFAULT NULL,
  `email_confirmed` tinyint(1) NULL DEFAULT 0,
  `email_confirmed_on` datetime(0) NULL DEFAULT NULL,
  `registered_on` datetime(0) NULL DEFAULT NULL,
  `last_logged_in` datetime(0) NULL DEFAULT NULL,
  `current_logged_in` datetime(0) NULL DEFAULT NULL,
  `role` varchar(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT 'user',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id_UNIQUE`(`id`) USING BTREE,
  UNIQUE INDEX `email_UNIQUE`(`email`) USING BTREE,
  INDEX `id`(`id`) USING BTREE,
  INDEX `email`(`email`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 12 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;

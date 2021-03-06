CREATE TABLE IF NOT EXISTS DF_REPOSITORY
(
    REPO_ID           INT(8) NOT NULL AUTO_INCREMENT,
    REPO_NAME         VARCHAR(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
    CH_REPO_NAME      VARCHAR(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
    CLASS             CHAR(32) NOT NULL,
    LABEL             CHAR(32) NOT NULL ,
    CREATE_USER       VARCHAR(64) NOT NULL,
    DESCRIPTION       VARCHAR(1024) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
    CREATE_TIME       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UPDATE_TIME       TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    STATUS            VARCHAR(2) NOT NULL,
    IMAGE_URL         VARCHAR(255) NOT NULL,
    PRIMARY KEY (REPO_ID),
    UNIQUE (REPO_NAME)

)  DEFAULT CHARSET=UTF8;

CREATE TABLE IF NOT EXISTS DF_DATAITEM (
  ITEM_ID       INT(8) NOT NULL AUTO_INCREMENT,
  ITEM_NAME     VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  REPO_NAME     VARCHAR(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  URL           VARCHAR(255) NOT NULL,
  CREATE_TIME   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UPDATE_TIME   TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP ,
  STATUS        VARCHAR(2) NOT NULL,
  SIMPLE        VARCHAR(1024) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (ITEM_ID),
  CONSTRAINT `FK_REPO_NAME` FOREIGN KEY (REPO_NAME) REFERENCES DF_REPOSITORY (REPO_NAME) 
    ON UPDATE CASCADE,
  CONSTRAINT `UK_REPO_ITEM` UNIQUE (REPO_NAME, ITEM_NAME)

)  DEFAULT CHARSET=UTF8;

CREATE TABLE IF NOT EXISTS DF_ATTRIBUTE
(
   ATTR_ID     INT(11) NOT NULL AUTO_INCREMENT,
   ITEM_ID     INT(8) NOT NULL,
   ATTR_NAME   VARCHAR(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
   INSTRUCTION VARCHAR(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
   ORDER_ID    INT(8) NOT NULL,
   EXAMPLE     VARCHAR(512) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
   PRIMARY KEY (ATTR_ID),
   CONSTRAINT `FK_ITEM_ID` FOREIGN KEY (ITEM_ID) REFERENCES DF_DATAITEM (ITEM_ID) 
     ON UPDATE CASCADE

)  DEFAULT CHARSET=UTF8;

CREATE TABLE IF NOT EXISTS DF_ITEM_STAT
(
   STAT_KEY     VARCHAR(255) NOT NULL COMMENT '3*255 = 765 < 767',
   STAT_VALUE   INT NOT NULL,
   PRIMARY KEY (STAT_KEY)

)  DEFAULT CHARSET=UTF8;


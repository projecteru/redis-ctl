DROP TABLE IF EXISTS `cluster`;

CREATE TABLE `cluster` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `description` text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `redis_node`;

CREATE TABLE `redis_node` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `host` char(24) NOT NULL,
  `port` int(11) NOT NULL,
  `max_mem` bigint(20) NOT NULL,
  `status` tinyint(4) NOT NULL,
  `assignee_id` int(11) DEFAULT NULL,
  `occupier_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `address_index` (`host`,`port`),
  UNIQUE KEY `occupier_id` (`occupier_id`),
  KEY `assignee_id` (`assignee_id`),
  CONSTRAINT `redis_node_ibfk_1` FOREIGN KEY (`assignee_id`) REFERENCES `cluster` (`id`),
  CONSTRAINT `redis_node_ibfk_2` FOREIGN KEY (`occupier_id`) REFERENCES `cluster` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


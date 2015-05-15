ALTER TABLE `redis_node` ADD COLUMN `eru_container_id` CHAR(64);
ALTER TABLE `redis_node` ADD COLUMN `eru_image_sha` CHAR(40);

CREATE INDEX `node_eru_container_id` ON `redis_node` (`eru_container_id`);

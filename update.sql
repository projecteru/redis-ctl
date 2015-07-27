ALTER TABLE `redis_node` ADD COLUMN `eru_container_id` CHAR(64);
CREATE INDEX `node_eru_container_id` ON `redis_node` (`eru_container_id`);

ALTER TABLE `proxy` ADD COLUMN `eru_container_id` CHAR(64);
CREATE INDEX `proxy_eru_container_id` ON `proxy` (`eru_container_id`);

ALTER TABLE `redis_node` DROP COLUMN `max_mem`;

ALTER TABLE `redis_node` MODIFY `host` VARCHAR(255);
ALTER TABLE `proxy` MODIFY `host` VARCHAR(255);
ALTER TABLE `redis_node_status` MODIFY `addr` VARCHAR(255);
ALTER TABLE `proxy_status` MODIFY `addr` VARCHAR(255);

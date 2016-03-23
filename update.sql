ALTER TABLE `redis_node` ADD COLUMN `eru_container_id` CHAR(64);
CREATE INDEX `node_eru_container_id` ON `redis_node` (`eru_container_id`);

ALTER TABLE `proxy` ADD COLUMN `eru_container_id` CHAR(64);
CREATE INDEX `proxy_eru_container_id` ON `proxy` (`eru_container_id`);

ALTER TABLE `redis_node` DROP COLUMN `max_mem`;

ALTER TABLE `redis_node` MODIFY `host` VARCHAR(255);
ALTER TABLE `proxy` MODIFY `host` VARCHAR(255);
ALTER TABLE `redis_node_status` MODIFY `addr` VARCHAR(255);
ALTER TABLE `proxy_status` MODIFY `addr` VARCHAR(255);

-- 2016-04 0.9.0

ALTER TABLE `cluster_task` ADD COLUMN `user_id` INT(11);
CREATE INDEX `cluster_task_creator` ON `cluster_task` (`user_id`);

ALTER TABLE `cluster` ADD COLUMN `creation` DATETIME NOT NULL;
UPDATE `cluster` SET `creation` = NOW();

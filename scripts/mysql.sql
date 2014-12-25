drop table if exists `redis_node`;
drop table if exists `cluster`;

create table `cluster` (
    `id` int unique not null auto_increment,
    `description` text not null,
    primary key(`id`)
);

create table `redis_node` (
    `id` int unique not null auto_increment,
    `host` char(24) not null,
    `port` int not null,
    `max_mem` bigint not null,
    `status` tinyint not null,
    `assignee_id` int,
    `occupier_id` int unique,
    foreign key (`assignee_id`) references `cluster`(`id`),
    foreign key (`occupier_id`) references `cluster`(`id`),
    primary key(`id`)
);

alter table `redis_node` add unique `address_index`(`host`, `port`);

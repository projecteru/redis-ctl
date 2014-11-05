drop table if exists `cache_instance`;
drop table if exists `application`;

create table `application` (
    `id` int unique not null auto_increment,
    `app_name` varchar(128) unique not null,
    primary key(`id`)
);

create table `cache_instance` (
    `id` int unique not null auto_increment,
    `host` char(24) not null,
    `port` int not null,
    `max_mem` bigint not null,
    `status` tinyint not null,
    `assignee_id` int,
    `occupier_id` int unique,
    foreign key (`assignee_id`) references `application`(`id`),
    foreign key (`occupier_id`) references `application`(`id`),
    primary key(`id`)
);

alter table `cache_instance` add unique `address_index`(`host`, `port`);

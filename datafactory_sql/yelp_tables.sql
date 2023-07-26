/*
DROP TABLE IF EXISTS yelp_checkin;
DROP TABLE IF EXISTS yelp_tip;
DROP TABLE IF EXISTS yelp_review;
DROP TABLE IF EXISTS yelp_business;
DROP TABLE IF EXISTS yelp_user;
*/
CREATE TABLE yelp_review_user_business_checkin 

CREATE TABLE delta_bronze_table (
    review_id NVARCHAR(30),
    user_id NVARCHAR(30),
    business_id NVARCHAR(30),
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    text NVARCHAR(MAX),
    date DATETIME,
    business_name NVARCHAR(100),
    address NVARCHAR(200),
    city NVARCHAR(100),
    state NVARCHAR(10),
    postal_code NVARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    business_stars FLOAT,
    review_count INT,
    is_open INT,
    business_attributes NVARCHAR(2000),
    categories NVARCHAR(1000),
    hours NVARCHAR(500),
    yelping_since NVARCHAR(30),
    user_review_count INT,
    user_useful INT,
    user_funny INT,
    user_cool INT,
    elite NVARCHAR(100),
    friends NVARCHAR(MAX),
    fans INT,
    average_stars FLOAT,
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT,
    checkin_date NVARCHAR(MAX),
    tip_text NVARCHAR(500),
    tip_date DATETIME,
    tip_compliment_count INT
);



CREATE TABLE yelp_checkin (
    business_id NVARCHAR(30),
    date NVARCHAR(MAX)
    , ingestUTC datetime
);

CREATE TABLE yelp_tip (
    user_id NVARCHAR(30),
    business_id NVARCHAR(30),
    text NVARCHAR(500),
    date DATETIME,
    compliment_count INT
    , ingestUTC datetime
);

CREATE TABLE yelp_review (
    review_id NVARCHAR(30),
    user_id NVARCHAR(30),
    business_id NVARCHAR(30),
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    text NVARCHAR(MAX),
    date DATETIME
    , ingestUTC datetime
);

CREATE TABLE yelp_business (
    business_id NVARCHAR(30),
    name NVARCHAR(100),
    address NVARCHAR(200),
    city NVARCHAR(100),
    state NVARCHAR(10),
    postal_code NVARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INT,
    is_open INT,
    attributes NVARCHAR(2000),
    categories NVARCHAR(1000),
    hours NVARCHAR(500)
    , ingestUTC datetime
);

CREATE TABLE yelp_user (
    user_id NVARCHAR(30),
    name NVARCHAR(50),
    review_count INT,
    yelping_since NVARCHAR(30),
    useful INT,
    funny INT,
    cool INT,
    elite NVARCHAR(100),
    friends NVARCHAR(MAX),
    fans INT,
    average_stars FLOAT,
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT
    , ingestUTC datetime
);






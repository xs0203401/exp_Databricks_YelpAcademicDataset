/*
DROP TABLE IF EXISTS yelp_checkin;
DROP TABLE IF EXISTS yelp_tip;
DROP TABLE IF EXISTS yelp_review;
DROP TABLE IF EXISTS yelp_business;
DROP TABLE IF EXISTS yelp_user;
*/


CREATE TABLE yelp_checkin (
    business_id NVARCHAR(MAX),
    date NVARCHAR(MAX)
    , ingestUTC datetime
);

CREATE TABLE yelp_tip (
    user_id NVARCHAR(MAX),
    business_id NVARCHAR(MAX),
    text NVARCHAR(MAX),
    date DATETIME,
    compliment_count INT
    , ingestUTC datetime
);

CREATE TABLE yelp_review (
    review_id NVARCHAR(MAX),
    user_id NVARCHAR(MAX),
    business_id NVARCHAR(MAX),
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    text NVARCHAR(MAX),
    date DATETIME
    , ingestUTC datetime
);

CREATE TABLE yelp_business (
    business_id NVARCHAR(MAX),
    name NVARCHAR(MAX),
    address NVARCHAR(MAX),
    city NVARCHAR(MAX),
    state NVARCHAR(MAX),
    postal_code NVARCHAR(MAX),
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INT,
    is_open INT,
    attributes NVARCHAR(MAX),
    categories NVARCHAR(MAX),
    hours NVARCHAR(MAX)
    , ingestUTC datetime
);

CREATE TABLE yelp_user (
    user_id NVARCHAR(MAX),
    name NVARCHAR(MAX),
    review_count INT,
    yelping_since NVARCHAR(MAX),
    useful INT,
    funny INT,
    cool INT,
    elite NVARCHAR(MAX),
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






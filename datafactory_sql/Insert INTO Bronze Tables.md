# Insert INTO Bronze Tables

User
In delta lake proces, I have the following source staging tables:

```
CREATE TABLE stg_yelp_business (
        business_id NVARCHAR(30) PRIMARY KEY,
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

    CREATE TABLE stg_yelp_checkin (
        business_id NVARCHAR(30) PRIMARY KEY,
        date NVARCHAR(MAX)
        , ingestUTC datetime
    );

    CREATE TABLE stg_yelp_tip (
        user_id NVARCHAR(30),
        business_id NVARCHAR(30),
        text NVARCHAR(500),
        date DATETIME,
        compliment_count INT
        , ingestUTC datetime
    );

    CREATE TABLE stg_yelp_review (
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

    CREATE TABLE stg_yelp_user (
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
```

And here is my bronze table:

```
CREATE TABLE delta_bronze_table (
    row_id INT IDENTITY(1,1) PRIMARY KEY,
    source_table_name NVARCHAR(20),
    review_id NVARCHAR(30),
    user_id NVARCHAR(30),
    business_id NVARCHAR(30),
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    [text] NVARCHAR(MAX),
    [date] DATETIME,
    [name] NVARCHAR(100),
    [address] NVARCHAR(200),
    city NVARCHAR(100),
    [state] NVARCHAR(10),
    postal_code NVARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    business_stars FLOAT,
    review_count INT,
    is_open INT,
    attributes NVARCHAR(2000),
    categories NVARCHAR(1000),
    [hours] NVARCHAR(500),
    checkin_date NVARCHAR(MAX),
    compliment_count INT,
    yelping_since NVARCHAR(30),
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
    ingestUTC datetime
);
```

Can you generate the insert statements for load each 5 source tables to my bronze table?

ChatGPT
Sure, I can generate the INSERT statements for loading each of the 5 source tables into your bronze table. Assuming you have the Delta Lake set up and ready, you can use the following INSERT statements for each table:

stg_yelp_business to delta_bronze_table:
sql
Copy code

```
INSERT INTO delta_bronze_table (
    source_table_name,
    [name],
    [address],
    city,
    [state],
    postal_code,
    latitude,
    longitude,
    business_id,
    business_stars,
    review_count,
    is_open,
    attributes,
    categories,
    [hours],
    ingestUTC
)
SELECT
    'business',
    [name],
    [address],
    city,
    [state],
    postal_code,
    latitude,
    longitude,
    business_id,
    stars,
    review_count,
    is_open,
    attributes,
    categories,
    [hours],
    ingestUTC
FROM stg_yelp_business;
```

stg_yelp_checkin to delta_bronze_table:
sql
Copy code

```
INSERT INTO delta_bronze_table (
    source_table_name,
    business_id,
    checkin_date,
    ingestUTC
)
SELECT
    'checkin',
    business_id,
    date,
    ingestUTC
FROM stg_yelp_checkin;
```

stg_yelp_tip to delta_bronze_table:
sql
Copy code

```
INSERT INTO delta_bronze_table (
    source_table_name,
    user_id,
    business_id,
    [text],
    [date],
    compliment_count,
    ingestUTC
)
SELECT
    'tip',
    user_id,
    business_id,
    [text],
    [date],
    compliment_count,
    ingestUTC
FROM stg_yelp_tip;
```

stg_yelp_review to delta_bronze_table:
sql
Copy code

```
INSERT INTO delta_bronze_table (
    source_table_name,
    review_id,
    user_id,
    business_id,
    stars,
    useful,
    funny,
    cool,
    [text],
    [date],
    ingestUTC
)
SELECT
    'review',
    review_id,
    user_id,
    business_id,
    stars,
    useful,
    funny,
    cool,
    [text],
    [date],
    ingestUTC
FROM stg_yelp_review;
```

stg_yelp_user to delta_bronze_table:
sql
Copy code

```
INSERT INTO delta_bronze_table (
    source_table_name,
    user_id,
    [name],
    review_count,
    yelping_since,
    useful,
    funny,
    cool,
    elite,
    friends,
    fans,
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos,
    ingestUTC
)
SELECT
    'user',
    user_id,
    [name],
    review_count,
    yelping_since,
    useful,
    funny,
    cool,
    elite,
    friends,
    fans,
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos,
    ingestUTC
FROM stg_yelp_user;
```

Make sure you have the appropriate permissions and ensure that the column order and data types match between the source tables and the bronze table to avoid any insert errors. Additionally, consider running these statements within a transaction or use other methods to ensure data consistency.

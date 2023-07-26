-- Insert into Bronze Table
CREATE PROCEDURE dbo.LoadDataToBronzeTable
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    -- Load from yelp_business
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

    -- load from yelp_checkin
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

    -- load from yelp_tip
    INSERT INTO delta_bronze_table (
        source_table_name,
        [user_id],
        business_id,
        [text],
        [date],
        compliment_count,
        ingestUTC
    )
    SELECT
        'tip',
        [user_id],
        business_id,
        [text],
        [date],
        compliment_count,
        ingestUTC
    FROM stg_yelp_tip;

    -- load from yelp_review
    INSERT INTO delta_bronze_table (
        source_table_name,
        review_id,
        [user_id],
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
        [user_id],
        business_id,
        stars,
        useful,
        funny,
        cool,
        [text],
        [date],
        ingestUTC
    FROM stg_yelp_review;

    -- load from yelp_user
    INSERT INTO delta_bronze_table (
        source_table_name,
        [user_id],
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
        [user_id],
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

    COMMIT TRANSACTION;
END;
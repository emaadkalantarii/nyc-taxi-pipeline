\c nyc_taxi;

DROP TABLE IF EXISTS daily_summary;
DROP TABLE IF EXISTS hourly_stats;
DROP TABLE IF EXISTS location_stats;
DROP TABLE IF EXISTS payment_stats;

CREATE TABLE hourly_stats (
    id                  SERIAL PRIMARY KEY,
    pickup_month        INTEGER NOT NULL,
    pickup_day_of_week  INTEGER NOT NULL,
    pickup_hour         INTEGER NOT NULL,
    time_of_day         VARCHAR(20),
    is_weekend          BOOLEAN,
    total_trips         BIGINT,
    avg_distance_miles  NUMERIC(10, 2),
    avg_duration_minutes NUMERIC(10, 2),
    avg_fare            NUMERIC(10, 2),
    total_revenue       NUMERIC(15, 2),
    avg_tip_pct         NUMERIC(10, 2),
    avg_speed_mph       NUMERIC(10, 2),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE location_stats (
    id              SERIAL PRIMARY KEY,
    pu_location_id  INTEGER NOT NULL,
    total_pickups   BIGINT,
    avg_fare        NUMERIC(10, 2),
    avg_distance    NUMERIC(10, 2),
    avg_tip_pct     NUMERIC(10, 2),
    total_revenue   NUMERIC(15, 2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payment_stats (
    id                SERIAL PRIMARY KEY,
    payment_type_desc VARCHAR(50),
    pickup_month      INTEGER NOT NULL,
    total_trips       BIGINT,
    avg_fare          NUMERIC(10, 2),
    avg_tip           NUMERIC(10, 2),
    total_revenue     NUMERIC(15, 2),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daily_summary (
    id                   SERIAL PRIMARY KEY,
    trip_date            DATE NOT NULL,
    pickup_month         INTEGER NOT NULL,
    total_trips          BIGINT,
    daily_revenue        NUMERIC(15, 2),
    avg_distance         NUMERIC(10, 2),
    avg_fare             NUMERIC(10, 2),
    active_pickup_zones  INTEGER,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_hourly_stats_month ON hourly_stats(pickup_month);
CREATE INDEX idx_hourly_stats_hour ON hourly_stats(pickup_hour);
CREATE INDEX idx_location_stats_location ON location_stats(pu_location_id);
CREATE INDEX idx_payment_stats_month ON payment_stats(pickup_month);
CREATE INDEX idx_daily_summary_date ON daily_summary(trip_date);
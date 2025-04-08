DELIMITER //

CREATE PROCEDURE StressTestWithMetricsandTime(
    IN duration_seconds INT, -- Total time to run the test in seconds
    IN batch_size INT        -- Number of rows to process per batch
)
BEGIN
    DECLARE start_time DATETIME;
    DECLARE elapsed_seconds INT DEFAULT 0;
    DECLARE total_transactions INT DEFAULT 0;
    DECLARE total_new_orders INT DEFAULT 0;
	DECLARE i INT DEFAULT 0;

    -- Create the test table if it doesn't exist
    CREATE TABLE IF NOT EXISTS stress_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        random_data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Record the start time
    SET start_time = NOW();

    -- Loop until the elapsed time exceeds the duration
    WHILE elapsed_seconds < duration_seconds DO
        -- INSERT (New Orders) in a batch
        START TRANSACTION;

        WHILE i < batch_size DO
            INSERT INTO stress_test (random_data)
            VALUES (CONCAT('Data ', RAND() * 10000));
            SET i = i + 1;
            SET total_new_orders = total_new_orders + 1; -- Count new orders
        END WHILE;
        COMMIT;

        -- Increment the transaction counter
        SET total_transactions = total_transactions + 1;

        -- UPDATE operations in a batch
        START TRANSACTION;
        UPDATE stress_test
        SET random_data = CONCAT(random_data, ' updated')
        ORDER BY RAND()
        LIMIT batch_size;
        COMMIT;

        -- Increment the transaction counter
        SET total_transactions = total_transactions + 1;

        -- DELETE operations in a batch
        START TRANSACTION;
        DELETE FROM stress_test
        ORDER BY RAND()
        LIMIT batch_size;
        COMMIT;

        -- Increment the transaction counter
        SET total_transactions = total_transactions + 1;

        -- Calculate elapsed time
        SET elapsed_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW());
    END WHILE;

    -- Output metrics
    SELECT
        total_transactions AS Total_Transactions,
        ROUND(total_transactions / (duration_seconds / 60), 2) AS TPM, -- Transactions Per Minute
        total_new_orders AS Total_New_Orders,
        ROUND(total_new_orders / (duration_seconds / 60), 2) AS NOPM;  -- New Orders Per Minute
END //

DELIMITER ;

Call StressTestWithMetrics(120, 5000)
DELIMITER //
DROP PROCEDURE IF EXISTS StressTest//
CREATE PROCEDURE StressTest(
    IN num_operations INT,
    IN batch_size INT
)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE j INT DEFAULT 0;
    -- Create a test table if not exists
    CREATE TABLE IF NOT EXISTS stress_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        random_data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    -- Insert operations
    WHILE i < num_operations DO
        SET j = 0;
        START TRANSACTION;
        WHILE j < batch_size DO
            INSERT INTO stress_test (random_data)
            VALUES (CONCAT('Data ', RAND() * 10000));
            SET j = j + 1;
        END WHILE;
        COMMIT;
        SET i = i + batch_size;
    END WHILE;
    -- Update operations
    SET i = 0;
    WHILE i < num_operations DO
        UPDATE stress_test
        SET random_data = CONCAT(random_data, ' updated')
        ORDER BY RAND()
        LIMIT batch_size;
        SET i = i + batch_size;
    END WHILE;
    -- Delete operations
    SET i = 0;
    WHILE i < num_operations DO
        DELETE FROM stress_test
        ORDER BY RAND()
        LIMIT batch_size;
        SET i = i + batch_size;
    END WHILE;
END //
 
DELIMITER ;

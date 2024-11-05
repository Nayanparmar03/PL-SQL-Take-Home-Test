CREATE OR REPLACE PACKAGE BODY stock_ownership_pkg AS

    -- Load data from exchange CSV file into exchange_ownership table
PROCEDURE load_exchange_data(filepath IN VARCHAR2) IS
        file_handle UTL_FILE.FILE_TYPE;
        line VARCHAR2(1000);
        user_id NUMBER;
        stock_id VARCHAR2(10);
        stock_name VARCHAR2(50);
        stock_count NUMBER;
    BEGIN
        file_handle := UTL_FILE.FOPEN(filepath, 'exchange_data.csv', 'r');
        
        LOOP
            BEGIN
                UTL_FILE.GET_LINE(file_handle, line);
                IF line IS NULL THEN
                    CONTINUE;
                END IF;
                
                -- Parse line data
                user_id := TO_NUMBER(REGEXP_SUBSTR(line, '[^,]+', 1, 1));
                stock_id := REGEXP_SUBSTR(line, '[^,]+', 1, 2);
                stock_name := REGEXP_SUBSTR(line, '[^,]+', 1, 3);
                stock_count := TO_NUMBER(REGEXP_SUBSTR(line, '[^,]+', 1, 4));
                
                INSERT INTO exchange_ownership (user_id, stock_id, stock_name, stock_count)
                VALUES (user_id, stock_id, stock_name, stock_count);
                
            EXCEPTION
                WHEN VALUE_ERROR THEN
                    DBMS_OUTPUT.PUT_LINE('Invalid format in exchange data row: ' || line);
                    CONTINUE;
            END;
        END LOOP;
        
        UTL_FILE.FCLOSE(file_handle);
    EXCEPTION
        WHEN UTL_FILE.INVALID_PATH THEN
            DBMS_OUTPUT.PUT_LINE('Invalid file path for exchange data.');
        WHEN UTL_FILE.INVALID_OPERATION THEN
            DBMS_OUTPUT.PUT_LINE('Error opening exchange data file.');
END load_exchange_data;

    -- Load data from depository CSV file into depository_ownership table
PROCEDURE load_depository_data(filepath IN VARCHAR2) IS
        file_handle UTL_FILE.FILE_TYPE;
        line VARCHAR2(1000);
        user_id NUMBER;
        stock_id VARCHAR2(10);
        stock_name VARCHAR2(50);
        stock_count NUMBER;
    BEGIN
        file_handle := UTL_FILE.FOPEN(filepath, 'depository_data.csv', 'r');
        
        LOOP
            BEGIN
                UTL_FILE.GET_LINE(file_handle, line);
                IF line IS NULL THEN
                    CONTINUE;
                END IF;

                -- Parse line data
                user_id := TO_NUMBER(REGEXP_SUBSTR(line, '[^,]+', 1, 1));
                stock_id := REGEXP_SUBSTR(line, '[^,]+', 1, 2);
                stock_name := REGEXP_SUBSTR(line, '[^,]+', 1, 3);
                stock_count := TO_NUMBER(REGEXP_SUBSTR(line, '[^,]+', 1, 4));
                
                INSERT INTO depository_ownership (user_id, stock_id, stock_name, stock_count)
                VALUES (user_id, stock_id, stock_name, stock_count);
                
            EXCEPTION
                WHEN VALUE_ERROR THEN
                    DBMS_OUTPUT.PUT_LINE('Invalid format in depository data row: ' || line);
                    CONTINUE;
            END;
        END LOOP;
        
        UTL_FILE.FCLOSE(file_handle);
    EXCEPTION
        WHEN UTL_FILE.INVALID_PATH THEN
            DBMS_OUTPUT.PUT_LINE('Invalid file path for depository data.');
        WHEN UTL_FILE.INVALID_OPERATION THEN
            DBMS_OUTPUT.PUT_LINE('Error opening depository data file.');
END load_depository_data;

    -- Procedure to compare data between exchange and depository ownership tables
PROCEDURE compare_ownership IS
    CURSOR c_comparison IS
        SELECT e.user_id, e.stock_id, e.stock_count AS exchange_count,
               d.stock_count AS depository_count
          FROM exchange_ownership e
              LEFT JOIN depository_ownership d
                ON e.user_id = d.user_id AND e.stock_id = d.stock_id;

        v_result VARCHAR2(20);
        
    BEGIN
        FOR rec IN c_comparison LOOP
            IF rec.depository_count IS NULL THEN
                v_result := 'MISMATCH - Missing in Depository';
            ELSIF rec.exchange_count = rec.depository_count THEN
                v_result := 'MATCH';
            ELSE
                v_result := 'MISMATCH';
            END IF;
            
            INSERT INTO ownership_comparison_log (
                user_id, stock_id, exchange_count, depository_count, result) VALUES (
                rec.user_id, rec.stock_id, rec.exchange_count, rec.depository_count, v_result);
        END LOOP;
END compare_ownership;

END stock_ownership_pkg;
/

CREATE OR REPLACE PACKAGE stock_ownership_pkg AS
    PROCEDURE load_exchange_data(filepath IN VARCHAR2);
    PROCEDURE load_depository_data(filepath IN VARCHAR2);
    PROCEDURE compare_ownership;
END stock_ownership_pkg;
/
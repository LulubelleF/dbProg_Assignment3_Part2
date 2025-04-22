/* 
Program:        A3_Part2_Process_Transactions.sql
-- Author:      Gabriel, Mitzi, Nicole, Lulubelle
-- Course:      CPRG304 – Assignment 3 Part 2
-- Date:        April 2025
--
-- Purpose:
--   This anonymous PL/SQL block processes transactions from a holding table
--   NEW_TRANSACTIONS, applies valid ones to ACCOUNT, TRANSACTION_DETAIL, and
--   TRANSACTION_HISTORY, and logs the first error per bad transaction into
--   WKIS_ERROR_LOG.  Clean transactions are committed one?by?one; bad ones
--   remain in NEW_TRANSACTIONS for later correction.
--
-- Usage:
--   1. Run create/constraint/load scripts.
--   2. Load A3_test dataset (clean & erroneous).
--   3. SET SERVEROUTPUT ON; then execute this script.
--
-- Guidelines:
--   • Uses two explicit cursors: one for transaction headers, one for rows.
--   • No arrays, no stored subprograms, only one anonymous block.
--   • Constants C_D and C_C represent debit ('D') and credit ('C') types.
--   • Only the first error per transaction is logged; the code then CONTINUEs.
--   • Clean transactions are committed individually; others are left in place.
--   • A catch?all WHEN OTHERS logs any unforeseen runtime error.

*/


SET SERVEROUTPUT ON SIZE UNLIMITED;
DECLARE
  -- constants for transaction types
  C_D CONSTANT CHAR := 'D';
  C_C CONSTANT CHAR := 'C';


  -- fetch every distinct transaction number (including NULL)

  CURSOR cur_trans IS
    SELECT DISTINCT transaction_no
      FROM NEW_TRANSACTIONS;

  -- fetch all rows for a given transaction_no

  CURSOR cur_rows(p_no NUMBER) IS
    SELECT transaction_date,
           description,
           account_no,
           transaction_type,
           transaction_amount
      FROM NEW_TRANSACTIONS
     WHERE transaction_no = p_no;

  -- local vars
  v_trans_no     NEW_TRANSACTIONS.transaction_no%TYPE;
  v_date         NEW_TRANSACTIONS.transaction_date%TYPE;
  v_desc         NEW_TRANSACTIONS.description%TYPE;
  v_sum_debit    NUMBER;
  v_sum_credit   NUMBER;
  v_error_found  BOOLEAN;
  v_error_msg    VARCHAR2(200);
  v_def_trans    CHAR(1);
  v_acct_balance NUMBER;

  -- dummies for cursor fetch in NULL case
  v_dummy_acct   NEW_TRANSACTIONS.account_no%TYPE;
  v_dummy_type   NEW_TRANSACTIONS.transaction_type%TYPE;
  v_dummy_amt    NEW_TRANSACTIONS.transaction_amount%TYPE;
BEGIN
  FOR t IN cur_trans LOOP
    v_trans_no := t.transaction_no;


    -- 1) Handle NULL transaction_no via explicit cursor

    IF v_trans_no IS NULL THEN
      OPEN cur_rows(NULL);

FETCH cur_rows
  INTO v_date,
       v_desc,
       v_dummy_acct,
       v_dummy_type,
       v_dummy_amt;

CLOSE cur_rows;

      INSERT INTO WKIS_ERROR_LOG
        (transaction_no, transaction_date, description, error_msg)
      VALUES
        (NULL, v_date, v_desc, 'Missing transaction number');

      CONTINUE;
    END IF;


    -- 2) Validation Phase: reset accumulators and flags

    v_sum_debit   := 0;
    v_sum_credit  := 0;
    v_error_found := FALSE;
    v_desc        := NULL;

    FOR r IN cur_rows(v_trans_no) LOOP
      IF v_desc IS NULL THEN
        v_date := r.transaction_date;
        v_desc := r.description;
      END IF;

      IF r.transaction_type NOT IN (C_D, C_C) THEN
        v_error_found := TRUE;
        v_error_msg   := 'Invalid transaction type: ' || r.transaction_type;
        EXIT;
      END IF;

      IF r.transaction_amount < 0 THEN
        v_error_found := TRUE;
        v_error_msg   := 'Negative transaction amount: ' || r.transaction_amount;
        EXIT;
      END IF;

      BEGIN
        SELECT 1
          INTO v_acct_balance
          FROM ACCOUNT
         WHERE account_no = r.account_no;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          v_error_found := TRUE;
          v_error_msg   := 'Invalid account number: ' || r.account_no;
          EXIT;
      END;

      IF r.transaction_type = C_D THEN
        v_sum_debit := v_sum_debit + r.transaction_amount;
      ELSE
        v_sum_credit := v_sum_credit + r.transaction_amount;
      END IF;
    END LOOP;

    IF NOT v_error_found AND v_sum_debit <> v_sum_credit THEN
      v_error_found := TRUE;
      v_error_msg   := 'Debits(' || v_sum_debit || ') vs Credits(' || v_sum_credit || ')';
    END IF;


    -- 3) Log first validation error and skip

    IF v_error_found THEN
      INSERT INTO WKIS_ERROR_LOG
        (transaction_no, transaction_date, description, error_msg)
      VALUES
        (v_trans_no, v_date, v_desc, v_error_msg);
      CONTINUE;
    END IF;


    -- 4) Processing Phase: history first, then details, then delete

    INSERT INTO TRANSACTION_HISTORY
      (transaction_no, transaction_date, description)
    VALUES
      (v_trans_no, v_date, v_desc);

    FOR r IN cur_rows(v_trans_no) LOOP
      SELECT a.account_balance,
             at.default_trans_type
        INTO v_acct_balance, v_def_trans
        FROM ACCOUNT a
        JOIN ACCOUNT_TYPE at
          ON a.account_type_code = at.account_type_code
       WHERE a.account_no = r.account_no;

      IF r.transaction_type = v_def_trans THEN
        v_acct_balance := v_acct_balance + r.transaction_amount;
      ELSE
        v_acct_balance := v_acct_balance - r.transaction_amount;
      END IF;

      UPDATE ACCOUNT
         SET account_balance = v_acct_balance
       WHERE account_no = r.account_no;

      INSERT INTO TRANSACTION_DETAIL
        (account_no, transaction_no, transaction_type, transaction_amount)
      VALUES
        (r.account_no, v_trans_no, r.transaction_type, r.transaction_amount);
    END LOOP;

    DELETE FROM NEW_TRANSACTIONS
     WHERE transaction_no = v_trans_no;

    COMMIT;
  END LOOP;

EXCEPTION
  WHEN OTHERS THEN
    v_error_msg := SUBSTR(SQLERRM,1,200);
    INSERT INTO WKIS_ERROR_LOG
      (transaction_no, transaction_date, description, error_msg)
    VALUES
      (v_trans_no, v_date, v_desc, v_error_msg);
    ROLLBACK;
END;
/


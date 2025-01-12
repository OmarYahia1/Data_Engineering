--1
SELECT * FROM "fintech_data_NETW_P2_52_3224_clean" ORDER BY loan_amount DESC LIMIT 20;

--2
SELECT state, AVG(annual_inc) AS avg_income FROM "fintech_data_NETW_P2_52_3224_clean" GROUP BY state;

--3
SELECT state, AVG(int_rate) AS avg_int_rate FROM "fintech_data_NETW_P2_52_3224_clean" GROUP BY state ORDER BY avg_int_rate DESC LIMIT 1;

--4
SELECT state, AVG(int_rate) AS avg_int_rate FROM "fintech_data_NETW_P2_52_3224_clean" GROUP BY
 state ORDER BY avg_int_rate ASC LIMIT 1;

--5
SELECT DISTINCT ON (state) state, grade, COUNT(*) AS most_frequent FROM "fintech_data_NETW_P2_52_3224_clean" GROUP BY state, grade ORDER BY state, most_frequent DESC;

--6
SELECT state, AVG(grade) AS avg_status FROM "fintech_data_NETW_P2_52_3224_clean" GROUP BY state ORDER BY avg_status DESC LIMIT 1;

--7
SELECT AVG(loan_amount) AS avg_loan_amount FROM "fintech_data_NETW_P2_52_3224_clean" WHERE issue_date BETWEEN '2015-01-01' AND '2018-12-31';

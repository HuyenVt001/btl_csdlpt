1. thay password trong 2 file Interface và testHelper thành password của tài khoản root trong mysql workbench của từng người
2. tải file dữ liệu của thầy về và giải nén
3. trong file data, thay đường dẫn file đầu vào thành đường dẫn file ratings.dat của từng người, và thay đường dẫn file đầu ra tùy ý, nhưng giữ đặt tên file là small_ratings.dat
4. INPUT_FILE_PATH trong file Assignment là đường dẫn file đầu ra bên trên
5. cài thư viên pymysql và cryptography trước khi chạy (pip install pymysql, tt với tv còn lại)
6. chạy file Assignment.py và xem kết quả, nếu tất cả đã pass là code không có lỗi
7. kiểm tra dữ liệu các bảng trên mysql
8. sau khi chạy thành công, hãy đọc hiểu code từng phần
   Chú ý: nếu muốn test nhiều dữ liệu hơn có thể thay đổi số dòng dữ liệu được lấy trong file data.py và thay số dòng đó vào ACTUAL_ROWS_IN_INPUT_FILE trong file Assignment.py

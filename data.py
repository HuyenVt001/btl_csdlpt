with open('./ratings.dat', 'r', encoding='utf-8') as f_in: #file đầu vào
    lines = f_in.readlines()[:50]  # Lấy 50 dòng đầu
with open('./small_ratings.dat', 'w', encoding='utf-8') as f_out: #file xuất ra
    f_out.writelines(lines)
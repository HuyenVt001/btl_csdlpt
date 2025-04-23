with open('E:/Download/ml-10m/ml-10M100K/ratings.dat', 'r', encoding='utf-8') as f_in: #file đầu vào
    lines = f_in.readlines()[:50]  # Lấy 50 dòng đầu
with open('E:/Download/ml-10m/ml-10M100K/small_ratings.dat', 'w', encoding='utf-8') as f_out: #file xuất ra
    f_out.writelines(lines)
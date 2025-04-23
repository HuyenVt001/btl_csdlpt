with open('E:/Download/ml-10m/ml-10M100K/ratings.dat', 'r', encoding='utf-8') as f_in:
    lines = f_in.readlines()[:50]  # Lấy 20 dòng đầu
with open('E:/Download/ml-10m/ml-10M100K/small_ratings.dat', 'w', encoding='utf-8') as f_out:
    f_out.writelines(lines)
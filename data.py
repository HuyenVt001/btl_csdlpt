import config

with open(config.INPUT_DATA, 'r', encoding='utf-8') as f_in: #file đầu vào
    lines = f_in.readlines()[:config.ACTUAL_ROWS_IN_INPUT_FILE]  # Lấy 50 dòng đầu
with open(config.INPUT_FILE_PATH, 'w', encoding='utf-8') as f_out: #file xuất ra
    f_out.writelines(lines)
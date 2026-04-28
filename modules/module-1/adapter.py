import os, time, shutil, csv, logging
import mysql.connector

# Cấu hình log – ghi cảnh báo ra console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def connect_db():
    while True:
        try:
            conn = mysql.connector.connect(
                host="mysql_db", user="root", password="root", database="webstore"
            )
            logger.info("Kết nối MySQL thành công.")
            return conn
        except Exception as e:
            logger.warning(f"Chờ MySQL khởi động... ({e})")
            time.sleep(5)


def process():
    path = "/app/input/inventory.csv"
    if not os.path.exists(path):
        return  # Chưa có file, bỏ qua

    logger.info(f"Phát hiện file: {path} – bắt đầu xử lý.")
    conn   = connect_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0

    with open(path, "r") as f:
        for line_num, row in enumerate(csv.reader(f), start=1):
            try:
                p_id = int(row[0])
                qty  = int(row[1])
            except (ValueError, IndexError):
                logger.warning(
                    f"Dòng {line_num}: '{row}' – sai định dạng (không thể parse), bỏ qua."
                )
                skipped += 1
                continue

            if qty < 0:
                logger.warning(
                    f"Dòng {line_num}: product_id={p_id}, quantity={qty} < 0 – bỏ qua."
                )
                skipped += 1
                continue

            cursor.execute(
                "UPDATE products SET stock = %s WHERE id = %s", (qty, p_id)
            )
            updated += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Hoàn tất: {updated} dòng cập nhật, {skipped} dòng bị bỏ qua.")

    # Cleanup – đổi tên kèm timestamp rồi chuyển sang /app/processed
    dest = f"/app/processed/inventory_{int(time.time())}.csv"
    shutil.move(path, dest)
    logger.info(f"Đã chuyển file sang: {dest}")


if __name__ == "__main__":
    logger.info("Legacy Adapter khởi động – polling mỗi 10 giây.")
    while True:
        process()
        time.sleep(10)
-- Xóa bảng nếu tồn tại để chạy lại script dễ dàng (theo thứ tự phụ thuộc)
DROP TABLE IF EXISTS transactions; -- Xóa bảng tham chiếu trước
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS products;

-- Bảng Accounts (từ accounts.csv)
CREATE TABLE IF NOT EXISTS accounts (
    customer_id INT PRIMARY KEY,         -- Khóa chính
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    address_1 TEXT,                     -- Địa chỉ có thể dài
    address_2 TEXT,                     -- Địa chỉ phụ, có thể NULL
    city VARCHAR(100),
    state VARCHAR(50),                  -- Viết tắt tiểu bang/tỉnh
    zip_code VARCHAR(20),               -- Mã zip/bưu điện dạng chuỗi
    join_date DATE                      -- Ngày tham gia
);

-- Index cho các cột có thể dùng để tìm kiếm
CREATE INDEX IF NOT EXISTS idx_accounts_name ON accounts(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_accounts_city_state ON accounts(city, state);

-- Bảng Products (từ products.csv)
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,         -- Khóa chính
    product_code VARCHAR(50) UNIQUE,    -- Mã sản phẩm, nên là duy nhất
    product_description TEXT
);

-- Index cho product_code để tìm kiếm nhanh
CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code);

-- Bảng Transactions (từ transactions.csv)
CREATE TABLE IF NOT EXISTS transactions (
    -- <<< THAY ĐỔI KIỂU DỮ LIỆU Ở ĐÂY >>>
    transaction_id VARCHAR(255) PRIMARY KEY, -- Đổi từ INT sang VARCHAR
    transaction_date TIMESTAMP,         -- Ngày giờ giao dịch
    product_id INT,                     -- Khóa ngoại đến products
    product_code VARCHAR(50),           -- Lưu lại mã sản phẩm
    product_description TEXT,           -- Lưu lại mô tả
    quantity INT,
    account_id INT,                     -- Khóa ngoại đến accounts

    -- Khóa ngoại tham chiếu đến bảng accounts
    CONSTRAINT fk_account
        FOREIGN KEY(account_id)
        REFERENCES accounts(customer_id)
        ON DELETE SET NULL,

    -- Khóa ngoại tham chiếu đến bảng products
    CONSTRAINT fk_product
        FOREIGN KEY(product_id)
        REFERENCES products(product_id)
        ON DELETE SET NULL
);

-- Index cho khóa ngoại và ngày giao dịch
CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_transaction_date ON transactions(transaction_date);


GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'test'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;


CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    num INTEGER NOT NULL,
    `data` VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL
);

INSERT INTO
    test_data(num, `data`, timestamp)
VALUES
    (100, "hello", NOW()),
    (200, "world", NOW()),
    (300, "foo", NOW()),
    (400, "bar", NOW()),
    (500, "baz", NOW()),
    (600, "qux", NOW())
;

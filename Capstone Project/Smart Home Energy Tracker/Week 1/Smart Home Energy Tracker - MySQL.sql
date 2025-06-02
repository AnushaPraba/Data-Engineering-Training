create database smarthome_tracker;
use smarthome_tracker;

CREATE TABLE rooms (
    room_id INT AUTO_INCREMENT PRIMARY KEY,
    room_name VARCHAR(50) NOT NULL
);

CREATE TABLE devices (
    device_id INT AUTO_INCREMENT PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    room_id INT,
    status ENUM('ON', 'OFF') DEFAULT 'OFF',
    FOREIGN KEY (room_id) REFERENCES rooms(room_id)
);

CREATE TABLE energy_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    device_id INT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    energy_used_kwh DECIMAL(6,3),
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

INSERT INTO rooms (room_name) VALUES 
('Living Room'),
('Bedroom'),
('Kitchen'),
('Bathroom'),
('Garage');

INSERT INTO devices (device_name, room_id, status) VALUES 
('Smart Light - Living Room', 1, 'ON'),
('Smart TV', 1, 'OFF'),
('Ceiling Fan - Bedroom', 2, 'ON'),
('Electric Kettle', 3, 'OFF'),
('Washing Machine', 5, 'OFF'),
('Hair Dryer', 4, 'OFF');

INSERT INTO energy_logs (device_id, timestamp, energy_used_kwh) VALUES
(1, '2025-06-01 08:00:00', 0.05),
(1, '2025-06-01 09:00:00', 0.06),
(2, '2025-06-01 20:00:00', 0.12),
(3, '2025-06-01 22:00:00', 0.08),
(4, '2025-06-01 07:00:00', 0.10),
(5, '2025-06-01 14:00:00', 0.75),
(6, '2025-06-01 09:30:00', 0.20),
(1, '2025-06-02 08:00:00', 0.07),
(3, '2025-06-02 22:00:00', 0.09);

-- CRUD operations
# Create/Insert - Done

# Read
select * from energy_logs;

# Update
UPDATE devices SET status = 'OFF' WHERE device_id = 1;

# Delete 
DELETE FROM devices WHERE device_id = 2;

-- Stored Procedure
DELIMITER //
CREATE PROCEDURE get_energy_usage_per_room_day (IN target_date DATE)
BEGIN
    SELECT 
        r.room_name,
        DATE(e.timestamp) AS usage_date,
        SUM(e.energy_used_kwh) AS total_kwh
    FROM 
        energy_logs e
        JOIN devices d ON e.device_id = d.device_id
        JOIN rooms r ON d.room_id = r.room_id
    WHERE 
        DATE(e.timestamp) = target_date
    GROUP BY 
        r.room_name, DATE(e.timestamp);
END //
DELIMITER ;

call get_energy_usage_per_room_day('2025-06-01');

<?php

$host = 'db_host';
$dbname = 'db_name';
$user = 'db_username';
$pass = 'db_password';
$pdo = "pgsql:host=$host;dbname=$dbname;user=$user;password=$pass";
$db = new PDO($pdo);

// API метод передачи данных из .csv файла в таблицу, задание 1
function insertDataFromCSV($db, $file) {
    $handle = fopen($file, "r");
    
    if ($handle !== false) {
        $insertQuery = "INSERT INTO your_table (number, name) VALUES ";
        $values = [];
        
        try {
            $db->beginTransaction();
            
            while (($data = fgetcsv($handle, 1000, ",")) !== false) {
                $splitData = splitRow($data);
                $number = $db->quote($splitData['number']);
                $name = $db->quote($splitData['name']);
                $values[] = "($number, $name)";
                
                if (count($values) >= 1000) {
                    $query = $insertQuery . implode(", ", $values);
                    $db->exec($query);
                    $values = [];
                }
            }
            
            if (!empty($values)) {
                $query = $insertQuery . implode(", ", $values);
                $db->exec($query);
            }
            
            $db->commit();
            fclose($handle);
            return ['success' => true];
        } catch (Exception $e) {
            $db->rollBack();
            fclose($handle);
            return ['success' => false, 'error_message' => $e->getMessage()];
        }
    }
    
    return ['success' => false];
}

// API метод рассылки данных из таблицы, задание 2
function sendDataFromTable($db) {
    $lastProcessedNumber = loadLastProcessedNumber();
    $errorOccurred = false;
    
    try {
        $db->beginTransaction();

        $rows = getRowsFromTable($db, $lastProcessedNumber);
        
        if (!empty($rows)) {
            foreach ($rows as $row) {
                $number = $row['number'];
                $name = $row['name'];
                
                $result = sendMethod($number, $name);
                
                if ($result === false) {
                    $errorOccurred = true;
                    saveLastProcessedNumber($lastProcessedNumber);
                    break;
                }
                
                $lastProcessedNumber = $number;
            }
        }

        if ($errorOccurred) {
            $db->rollBack();
            return ['success' => false, 'last_processed_number' => $lastProcessedNumber];
        }

        $db->commit();
        return ['success' => true];
    } catch (Exception $e) {
        $db->rollBack();
        return ['success' => false, 'error_message' => $e->getMessage()];
    }
}

// Метод сохранения последнего обработанного номера в файл
function saveLastProcessedNumber($number) {
    file_put_contents('last_processed_number.txt', $number);
}

// Метод загрузки последнего обработанного номера из файла
function loadLastProcessedNumber() {
    if (file_exists('last_processed_number.txt')) {
        return file_get_contents('last_processed_number.txt');
    }
    return '';
}

// Метод разбивки строки с разделителем ',' на две части
function splitRow($str) {
    $parts = explode(',', $str);
    $number = $parts[0];
    $name = $parts[1] ?? '';

    return ['number' => $name, 'name' => $name];
}

// Метод получения данных из таблицы
function getRowsFromTable($db, $lastProcessedNumber = '') {
    $query = "SELECT number, name FROM your_table";
    if (!empty($lastProcessedNumber)) {
        $query .= " WHERE number > '$lastProcessedNumber'";
    }

    $stmt = $db->query($query);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

// Метод отправки
function sendMethod($number, $name) {
    // код отправки

    return true;
}

?>
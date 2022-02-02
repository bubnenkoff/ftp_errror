import 'dart:io';

import 'package:data_loader/Models/ftp_connection_model.dart';
import 'package:ftpconnect/ftpconnect.dart';
import 'package:path/path.dart';
import 'package:postgres/postgres.dart';
import 'package:path/path.dart' as p;

late PostgreSQLConnection connection;
late FTPConnect ftpConnect;
late FTPCredentialsModel ftpCredential;

late bool loadOnlyOneRegionForTest;

PostgreSQLConnection getConnection() {
  return PostgreSQLConnection('localhost', 5432, 'fz44', username: 'postgres', password: 'Infinity8', queryTimeoutInSeconds: 3600);
}

FTPConnect getFTPConnection() {
  print("${ftpCredential.ftpUrl}, ${ftpCredential.ftpLogin}, ${ftpCredential.ftpPass}");
  print('---------');
  return FTPConnect(ftpCredential.ftpUrl, user: ftpCredential.ftpLogin, pass: ftpCredential.ftpPass, timeout: 30);
}

ftpReconnect() async {
    try {
        await ftpConnect.disconnect();
        ftpConnect = getFTPConnection();
        await ftpConnect.connect();
      } catch (e) {
        print('Exeption in ftpReconnect: $e');
        sleep(Duration(seconds: 1));
        ftpReconnect();
      }
          
}
    
    

late List<Map<dynamic, dynamic>> listOfMaps44fz;
late List<Map<dynamic, dynamic>> listOfMaps223fz;

// enum FTPType {fz44, fz223}

late DateTime startupTime; // чтобы знасть сколько мы уже работаем

Map serviceStatus = { // нужно для helthCheck нужно чтобы было доступно глобально было
              'serviceName': 'DataLoader',
              'uptime': null,
              'isDbConnected': null,

              'latestFTPScan': null,
              'latestArchiveLoad': null,
              'unprocessedArchivesCount': null,
              'unprocessedXMLCount': null,
              'currentTask': null,
              'latestError': null,
              'fz': null
             };

 

var context = p.Context(style: Style.posix);


late String currentlyProcessingFtpFz; // глобальный нужен т.к. в самом job идет перечисление fz 

const ARCHIVE_FOLDER_PATH = r"D:\zak_data";


Map<String, dynamic> ftpJob = {}; // нужно глобально

// Map ftpJob = {
// 	"fz": ["fz44", "fz223"],
// 	"sections": ["notifications", "contracts", "protocols"],
// 	"start_year": 2015,
//  "onlyLastMonth" : false,
//  "atLeastNotProcessedXML": 57000, // если не распакованных файлов меньше этого количества, то распаковать новые
// };

Map currentJob = {
  "filesInserted": 0, // сколько всего мы за один запуск парсера файлов вставили
  "achivesUnprocessedCount": 0,
  "lastRun": "" // дата последнего запуска парсера
  // "current"
};
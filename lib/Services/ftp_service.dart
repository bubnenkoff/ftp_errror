
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:data_loader/Models/ftp_connection_model.dart';
import 'package:data_loader/Models/ftp_item_model.dart';
import 'package:data_loader/ftp_credentials.dart';
import 'package:data_loader/globals.dart';
import 'package:data_loader/misc.dart';
import 'package:ftpconnect/ftpconnect.dart';
import 'package:postgres/postgres.dart';
import 'package:path/path.dart' as p;


FTPCredentialsModel selectFTP(String fzFTPName) {
  if(fzFTPName == 'fz44') {
      return FTPCredentialsModel.fromMap(FTPAuth['fz44']);
  }
  else {
      return FTPCredentialsModel.fromMap(FTPAuth['fz223']);
  }

}

// обходит все регионы и секции на FTP. Собирает пути до архивов
// чтобы обрабатывался только первый нужно break раскомментировать!!
getListOfArchives() async {
  serviceStatus['currentTask'] = 'getListOfArchives';

  print("getListOfArchives. Root folder: ${ftpCredential.rootFtpDir}");
  // await checkIfConnectedOrReconnectToFTP();
  try {
    await ftpConnect.changeDirectory(ftpCredential.rootFtpDir);
  } on Exception catch(e) {
    
    await ftpReconnect();

    await getListOfArchives();

  }

  List<FTPEntry> regionsEntries = []; 
  regionsEntries = await ftpConnect.listDirectoryContent(cmd: DIR_LIST_COMMAND.LIST); // FTPEntry комплексная структура, нужно извлечь нужное


  List<String> regionsNames = [];
 
  for(var el in regionsEntries) {
    
    if(el.type == FTPEntryType.DIR) {
      if (['PG-PZ', 'ERUZ', 'control99docs', 'temp_err', '_logs', 'fcs_undefined', 'undefined'].contains(el.name!)) continue;
      regionsNames.add(el.name!);
    }
  }

  for(var regionName in regionsNames) {
      List<FTPEntry> sectionEntries = []; 
      for(var sectionName in ftpJob['sections']) {

          if(serviceStatus['fz'] == 'fz223') {  // в 223 ФЗ секции немного иначе называются
            sectionName = sectionName.replaceAll('notifications', 'purchaseNotice');
            sectionName = sectionName.replaceAll('protocols', 'purchaseProtocol');
            sectionName = sectionName.replaceAll('contracts', 'purchaseContract');
          }

          String sectionPath;
          if(ftpJob['onlyLastMonth']) {
            if(serviceStatus['fz'] == 'fz223') {
                sectionPath = context.join(ftpCredential.rootFtpDir,regionName, sectionName, "daily");
            } else {
                sectionPath = context.join(ftpCredential.rootFtpDir,regionName, sectionName, "currMonth");
            }

          } else {
            sectionPath = context.join(ftpCredential.rootFtpDir,regionName, sectionName);
          }

          // теперь пути нужно поменять обратно, чтобы они были унифицированы
          if(serviceStatus['fz'] == 'fz223') {  // в 223 ФЗ секции немного иначе называются
            sectionName = sectionName.replaceAll('purchaseNotice', 'notifications');
            sectionName = sectionName.replaceAll('purchaseProtocol', 'protocols');
            sectionName = sectionName.replaceAll('purchaseContract', 'contracts');
          }

            await ftpConnect.changeDirectory(context.join(sectionPath));
            sectionEntries = await ftpConnect.listDirectoryContent(cmd: DIR_LIST_COMMAND.LIST); 

              for(var file in sectionEntries) {
                    if(file.type == FTPEntryType.FILE) {
                      print(file.name);
                      var regexp = RegExp(r'[0-9]{8}'); 
                      var match = regexp.firstMatch(file.name!);
                      var fileDate = match?.group(0);  // 20210901

                      if(fileDate != null) {
                        int fileYear = int.parse(fileDate.substring(0,4)); // 2018
                        if(fileYear >= ftpJob['start_year']) { // возможность использовать год который указали
                          ftpItemModelList.add(FTPItemModel(
                          regionName: regionName, 
                          sectionName: sectionName,
                          archiveName: file.name!,
                          ftpPath: sectionPath,
                          archDate: fileDate,
                          fz: currentlyProcessingFtpFz
                          ));
                        }
                    }
                  }
              
                }


      }

      if(loadOnlyOneRegionForTest) {
        break; // чтобы обрабатывать толко первый!!!
      }


  }

    serviceStatus['latestFTPScan'] = DateTime.now().toIso8601String();
    serviceStatus['currentTask'] = '';
    return ftpItemModelList;

}


// сохраняет в БД пути полученные в getListOfArchives
saveArchivesToDB() async {
  serviceStatus['currentTask'] = 'saveArchivesToDB';
  print('saveArchivesToDB');
  List<FTPItemModel> ftpArchives;
  try {
    ftpArchives = await getListOfArchives();
  } on Exception catch(e) {
    ftpArchives = await getListOfArchives();
  }

  print('Connection status: ${connection.isClosed}');
  if(connection.isClosed) { 
      connection = getConnection();
      await connection.open();
    }  

  for(var archFile in ftpArchives) {
    String sql = """
    INSERT INTO ftp_files ("region", "section_name", "ftp_path", "archive_name", "arch_date", "fz") 
    VALUES ('${archFile.regionName}', '${archFile.sectionName}', '${archFile.ftpPath}', '${archFile.archiveName}', to_timestamp ('${archFile.archDate}', 'yyyymmdd'), '${archFile.fz}') 
    ON CONFLICT ("archive_name") DO NOTHING;""".replaceAll('\n', '');
    //  print(sql);

    await connection.query(sql);
  }

  await connection.close();

}

// получаем один не обработанный архив из БД. Опционально можем указывать имя нужного региона
// возвращаем одну единственную модель! Не список моделей
Future<FTPItemModel?> getSingleNotUnpackedArchiveFromDB([String? region]) async {
  serviceStatus['currentTask'] = 'getSingleNotUnpackedArchiveFromDB';
  print('getSingleNotUnpackedArchiveFromDB');

  if(connection.isClosed) { 
      connection = getConnection();
      await connection.open();
    }  



  String sql;
  // формируем строку для подстановки в формате --> 'notifications','contracts','protocols'
  String sectionsWithSingleQuotes = json.encode(ftpJob['sections']).toString().replaceAll('"',"'").replaceAll('[', '').replaceAll(']', '');
  print(serviceStatus['fz']);
  if(region == null) {
    sql = """
         SELECT
            "region",
            "section_name",
            "ftp_path",
            "archive_name",
            "arch_date",
            "fz"
        FROM "ftp_files" WHERE "isUnpacked" IS NULL 
        AND date_part('year', arch_date) >= ${ftpJob['start_year']}
        AND "section_name" IN ($sectionsWithSingleQuotes) AND fz = '${serviceStatus['fz']}' ORDER BY "arch_date" LIMIT 1; 
        """;
  }

  else {
     sql = """
          SELECT
              "region",
              "section_name",
              "ftp_path",
              "archive_name",
              "arch_date",
              "fz"
          FROM "ftp_files" WHERE "isUnpacked" IS NULL 
          AND date_part('year', arch_date) >= ${ftpJob['start_year']}
          AND "section_name" IN ($sectionsWithSingleQuotes) AND region ='{$region}' AND fz = '${serviceStatus['fz']}' ORDER BY "arch_date" LIMIT 1; 
          """;
    }

    try {
      List<List<dynamic>> result = await connection.query(sql).timeout(Duration(seconds: 20));
      if (result.isNotEmpty) {
       
        // возможно в перспективе можно было бы и массив, но пока по одному
        // result[0] содержит все такой ответ [0] потому что мы всегда по одному файла запрашиваем.
        // [Adygeja_Resp, contracts, /fcs_regions/Adygeja_Resp/contracts/currMonth, contract_Adygeja_Resp_2020070800_2020071000_001.xml.zip, 2020-07-08 00:00:00.000Z, fz44]

        serviceStatus['currentTask'] = ''; // сбросим

        return FTPItemModel(
              regionName: result[0][0], 
              sectionName: result[0][1],
              ftpPath: result[0][2],
              archiveName: result[0][3],
              archDate: result[0][4].toString(),
              fz: result[0][5]
              );

      } else {
        print('COUNT: No Archives for processing'); 
        return null;
      }
    } on PostgreSQLException catch (e) {
      print(e);
      return null;
    }

  // print(sql);

}


Future downloadArchive() async {
  print("downloadArchive");
  serviceStatus['currentTask'] = 'downloadArchive';
  if(connection.isClosed) {  // databse connection
      connection = getConnection();
      await connection.open();
    }  


  FTPItemModel? singleArchive = await getSingleNotUnpackedArchiveFromDB();
  if(singleArchive == null) {
    var errorText = '[ERROR] Nothing do download. getSingleNotUnpackedArchiveFromDB return NULL';
    serviceStatus['latestError'] = errorText;
    print(errorText);
    return -1; // надо как-то сообщить выше что нечего загружать

    // не надо тут никаких исключений кидать т.к. все упадет если где-то выше их не ловить
    // throw Exception(errorText); // если вернулся нул, то значит вообще что-то не то
  }
  else {
      serviceStatus['latestError'] = '';
      print(ftpCredential.ftpUrl);
      String ftpArchiveFullPath = p.posix.join(singleArchive.ftpPath, singleArchive.archiveName);
      print("FTP remote path:  $ftpArchiveFullPath");
      
      // нужно создать папку если ее нет
      String localArchiveFolder = p.windows.join(ARCHIVE_FOLDER_PATH, serviceStatus['fz'], singleArchive.sectionName, singleArchive.regionName);
      print('localArchiveFolder: $localArchiveFolder');
      // serviceStatus['fz'] 
      if (!Directory(localArchiveFolder).existsSync()) {
        await Directory(localArchiveFolder).create(recursive: true);
        print("Folder created: $localArchiveFolder");
      }

      String localArchiveFullPath = p.windows.join(ARCHIVE_FOLDER_PATH, serviceStatus['fz'], singleArchive.sectionName, singleArchive.regionName, singleArchive.archiveName);
      print('local path: $localArchiveFullPath');

      // загружаем только если такого файла нет на файловой системе
      // почему-то иначе тут FTPException вылетал хз в чем причина
      if(!File(localArchiveFullPath).existsSync()) { 
        try {
          print("aa11");
          await ftpConnect.downloadFile(ftpArchiveFullPath, File(localArchiveFullPath));
          print("bb11");

        } on Exception catch (e) { // почему-то иногда выпадает и пробуем повторно чтобы файл разобрать
          print(e);  
          print("${ftpArchiveFullPath}");
          print('\nFTP Exception! we will call downloadArchive again: ${e} Second Attempt to download file'); 
          await ftpReconnect();
          print("11___________________11");
          await downloadArchive();

          
          // if(!File(ftpArchiveFullPath).existsSync()) { // если файл скачали но его нет, значит какая-то херня
          //   print("File DO NOT LOADED!!!");
          //  await downloadArchive(isRestartedByException = true);
          // }


        } 
      }

      print('File Downloaded: $localArchiveFullPath');
      // await ftpConnect.disconnect();  // мы еще закончили рано отключаться
      await unpackArchive(singleArchive, localArchiveFullPath);

      serviceStatus['latestArchiveLoad'] = DateTime.now().toIso8601String();
  }

    serviceStatus['currentTask'] = '';

}

// singleArchive передаем сюда т.к. в нем все метаданные регион, название секции и тд
unpackArchive(FTPItemModel singleArchive, String archiveFullPath) async {
  serviceStatus['currentTask'] = 'unpackArchive: ${singleArchive.archiveName}';

  print("unpackArchive");

  var file = File(archiveFullPath);
  int fileSize = file.lengthSync(); 
  if(fileSize < 2500) { 
    
    print("File $archiveFullPath is less than 2.5KB. Ignoring");
    await setArchiveUnpackFlagToDwarf(singleArchive);
  } else {
     String destinationPath =  archiveFullPath.replaceAll('.xml.zip', '');

      if (!Directory(destinationPath).existsSync()) {
        await Directory(destinationPath).create(recursive: true);
        print("Folder created: $destinationPath");
      }
    
     try { // на случай битых архивов
        await FTPConnect.unZipFile(File(archiveFullPath), destinationPath); 
     } on FormatException catch(e) {
      print(e.message);
        await setArchiveUnpackFlagToCorrupted(singleArchive);
        // и пробуем заново скачать
        await downloadArchive();
     }
     print("Unpacked: $destinationPath");
     
     await scanUnpackedFolder(singleArchive, destinationPath);
     print("scanfolder done");
     serviceStatus['currentTask'] = '';

    //  return; // !!!!
  }

}

// нужно загрузить в таблицу xml_files содержимое распакованного архива
// singleArchive передаем сюда т.к. в нем все метаданные регион, название секции и тд
scanUnpackedFolder(FTPItemModel singleArchive, String unpackedFolder) async {
  serviceStatus['currentTask'] = 'scanUnpackedFolder';
  print('scanning folder: $unpackedFolder');

  List<FileSystemEntity> files = await Directory(unpackedFolder).list().toList();

  for(var file in files) {
    
    int fileSize = file.statSync().size;
    if(fileSize < 2400) continue; // игнорируем слишком маленькие
    if(!file.path.endsWith('.xml')) continue; // пропускаем все что не xml
    
    List<String> excludeFileList = [
                                      'Clarification', // не будем захламлять БД данными содержащими ответ на запрос
                                      'fcsNotificationCancelFailure', // извещения об отмене определения поставщика пока не обрабатываем
                                      'notificationEFDateChange', // не ясно куда раскладывать информацию из него. Таких файлов крайне мало
                                      'fcsNotificationOrgChange', // в перспективе разработать логику для данных случаев таких типов мало. 
                                      'ClarificationResult', // примерно тоже самое
                                      'contractProcedure', // пока не совсем понятно как обрабатывать 
                                      'ProtocolCancel', // тут нет никакой информации полезной, просто игнорируем данный тип   
                                      'fcsContractSign' // пока непонятно какие данные отсюда можно брать 
                                      ];

    bool isContains = excludeFileList.any((e) => file.path.contains(e));
    if(isContains) continue; // пропускаем все файлы с ненужными паттернами

    List<String> purchaseNumAndDocDate = await simpleFieldsExtractor(file.path);

    print("___aaaaaaaa___");
    try {
      await insertOrUpdateXmlFiles(xmlName: p.basename(file.path), purchaseNumAndDocDate: purchaseNumAndDocDate, archiveItem: singleArchive);

    } on Exception catch (e) {
        if(connection.isClosed) { 
        connection = getConnection();
        await connection.open();
      }  
      await insertOrUpdateXmlFiles(xmlName: p.basename(file.path), purchaseNumAndDocDate: purchaseNumAndDocDate, archiveItem: singleArchive);
    }

    print("___bbbbbbbb___");

  }

      // только если мы вставили все файлы в БД, только тогда помечаем архив как обработанный
    await setArchiveUnpackFlagToTrue(singleArchive);

    serviceStatus['currentTask'] = '';

}

// из каждого файла пытаемся извлечь номер закупки и дату и вернуть их
Future<List<String>> simpleFieldsExtractor(String fileFullPath) async {
  serviceStatus['currentTask'] = 'simpleFieldsExtractor';

  // String file = "D:/zak_data/notifications/Adygeja_Resp/notification_Adygeja_Resp_2015010100_2015020100_001/fcsNotificationEA44_0176100001015000002_3586851.xml";

  File file = File(fileFullPath);
  String fileContent = File(fileFullPath).readAsStringSync();

  // у контрактов называется notificationNumber у остальных purchaseNumber
  List<String> purchaseNumber_xPathRulesList = [
                                                "purchaseNumber", 
                                                "notificationNumber",
                                                //223
                                                "registrationNumber"
                                                
                                                
                                                ];
  String? purchaseNumber;

  print('fileFullPath: $fileFullPath');
  purchaseNumber = extractValue(fileContent, purchaseNumber_xPathRulesList);
  print('purchaseNumber: $purchaseNumber');

  List<String> docPublishDate_xPathRulesList = [
                                                "docPublishDate", 
                                                "createDate",
                                                // "plannedPublishDate", лучше не использовать т.к. бывает кривой формат типа: 2020-12-28+03:00
                                                "docDT",
                                                "protocolDate",
                                                "publishDTInEIS",
                                                "publishDate",
                                                // 223
                                                "createDateTime"
                                                ];
  String? docPublishDate;

  docPublishDate = extractValue(fileContent, docPublishDate_xPathRulesList);
    
  print('$docPublishDate: docPublishDate');



  // в контрактах бывает так что может быть не указан, поэтому последний шанс
  //проверяем есть ли вообще где-то слово notificationNumber
  purchaseNumber ??= 'not_founded';


  if(purchaseNumber != null && docPublishDate != null) {
    serviceStatus['currentTask'] = '';
    return [purchaseNumber, docPublishDate];
  }

  else {
     if(docPublishDate == null) {
        print("[ERROR] Can't find rule for extracting docPublishDate: $fileFullPath");
        print('error in path: $fileFullPath');
        exit(0);
      }

    if(purchaseNumber == null) {
      print("[ERROR] Can't find rule for extracting purchaseNumber: $fileFullPath");
      print('error in path: $fileFullPath');
      exit(0);
    }
    
    
    throw Exception('xPath simpleFieldsExtractor cant process file $fileFullPath');
  }

  

}


insertOrUpdateXmlFiles(
  {required String xmlName, required List<String?> purchaseNumAndDocDate, required FTPItemModel archiveItem}) async {
  print("insertOrUpdateXmlFiles");
  // в xml_files мы вставляем region_id на базе таблицы регионов. 
  //По идее он foreign key должен быть, но мы его в данамике высчитываем загружая при старте все значения в переменные
  // ignore: non_constant_identifier_names

  if(connection.isClosed) { 
      connection = getConnection();
      await connection.open();
      print("insertOrUpdateXmlFiles DB reconnection");

    }  


  int? region_id;  
  
  if(archiveItem.fz == 'fz44') {
    region_id = listOfMaps44fz.singleWhere((element) => element['translite_name'] == archiveItem.regionName)['id'];
  }
  
  if(archiveItem.fz == 'fz223') {
    region_id = listOfMaps223fz.singleWhere((element) => element['translite_name'] == archiveItem.regionName)['id'];
  }  

  if(region_id == null) {
    throw Exception("you are trying to isert UNKNOW region: ${archiveItem.regionName}");
  }

  print('current region_id: $region_id');

  serviceStatus['currentTask'] = 'insertOrUpdateXmlFiles: $xmlName';
  // перед вставкой необходимо проверить, по имени определить action_type 
  // data_update или data_insert
  print('insertOrUpdateXmlFiles');
  String? purchaseNumber = purchaseNumAndDocDate[0];
  String? docDate = purchaseNumAndDocDate[1];

  var data_update_list = ["prolong", "cancel"];
  String action_type; 
  bool isContains = data_update_list.any((e) => xmlName.toLowerCase().contains(e));
    if(isContains) {
      action_type = 'data_update';
      // print('Action_type: data_update');
    } else {
      action_type = 'data_insert';
      // print('Action_type: data_insert');
    }

  String sql = """SELECT id, "docPublishDate", action_type FROM "xml_files" WHERE action_type = '$action_type' AND section_name = '${archiveItem.sectionName}' AND "purchaseNumber" = '$purchaseNumber' order by "docPublishDate" DESC Limit 1;""";
  // print(sql);

    int max = 10;
    int min = 1;
    Random rnd =  Random();
    int jobNumber = min + rnd.nextInt(max - min); // от 1 до 9

  try {
    // print(sql);
      print("aaa1");
      print(sql);
      List<List<dynamic>> result = await connection.query(sql).timeout(Duration(seconds: 120));
      print('bbb1');
      if (result.isEmpty) {
        // данных нет в БД и их нужно вставить
        // result[0] содержит все такой ответ [0] потому что мы всегда по одному файла запрашиваем.
        // xml_date - берем из даты архива
        sql = """INSERT INTO xml_files("arch_name", "file_name", "region", "section_name", "xml_date", "purchaseNumber", "docPublishDate", "fz", "jobNumber", "action_type", "region_id") 
        VALUES ('${archiveItem.archiveName.replaceAll('.xml.zip', '')}', '$xmlName', '${archiveItem.regionName}', '${archiveItem.sectionName}', '${archiveItem.archDate}', '$purchaseNumber', '${docDate}', '${archiveItem.fz}', '$jobNumber', '$action_type', $region_id) ON CONFLICT ("file_name") DO NOTHING;""";

        // print(sql);
        await connection.query(sql);
      } else {
        // если данные уже есть, то нужно понять старые данные в БД или новые и выставить exists_status "old" или пустой

        int dbXmlId = result[0][0];
        DateTime dbXMLDocDate = result[0][1]; // тип вернется из БД как DateTime


        if(dbXMLDocDate.isBefore(DateTime.parse(docDate!))) {
          // пометим все закупки младше этой как old
          sql = """UPDATE xml_files SET "exists_status" = 'old' WHERE action_type = 'data_update' AND section_name = '${archiveItem.sectionName}' AND "purchaseNumber"='$purchaseNumber' AND "docPublishDate" <'$docDate' """;
          // print(sql);
          await connection.query(sql);

          sql = """INSERT INTO xml_files("arch_name", "file_name", "region", "section_name", "xml_date", "purchaseNumber", "docPublishDate", "fz", "jobNumber", "action_type", region_id) 
          VALUES ('${archiveItem.archiveName.replaceAll('.xml.zip', '')}', '$xmlName', '${archiveItem.regionName}', '${archiveItem.sectionName}', '${archiveItem.archDate}', '$purchaseNumber', '$docDate', '${archiveItem.fz}', '$jobNumber', '$action_type', $region_id ) ON CONFLICT ("file_name") DO NOTHING;""";
          // print(sql2);
          currentJob['filesInserted']++; // увеличим счетчик обработанных файлов
          // print(sql);
          await connection.query(sql);
        } else { // сразу вставляем как old
  
          sql = """INSERT INTO xml_files("arch_name", "file_name", "region", "section_name", "xml_date", "purchaseNumber", "docPublishDate", "fz", "jobNumber", "action_type", "exists_status", "region_id") 
          VALUES ('${archiveItem.archiveName.replaceAll('.xml.zip', '')}', '$xmlName', '${archiveItem.regionName}', '${archiveItem.sectionName}', '${archiveItem.archDate}', '$purchaseNumber', '$docDate', '${archiveItem.fz}', '$jobNumber', '$action_type', 'old', $region_id ) ON CONFLICT ("file_name") DO NOTHING;""";
          // print(sql);
          await connection.query(sql);
        }

        serviceStatus['currentTask'] = '';

      }
    } on PostgreSQLException catch (e) {
      print('Exception during Insert in xml_files: $e');
      // exit(0);
      rethrow;
    } on SocketException catch(e) { // пытаемся багу поймать
      print('SocketException during Insert in xml_files: $e');
      // exit(0);
      rethrow;

    } on Exception catch(e) {
       print('BaseException during Insert in xml_files: $e');
      //  exit(0);
      rethrow;
    }


  
}

Future<int> getCountOfUnprocessedArchives(String sectionName) async {
  serviceStatus['currentTask'] = 'getCountOfUnprocessedArchives';

  // String sectionsWithSingleQuotes = json.encode(ftpJob['sections']).toString().replaceAll('"',"'").replaceAll('[', '').replaceAll(']', '');
  // проверяем сколько осталось для каждой секции отдельно
  // код выше для всех сразу из джоба проверяет
  String sectionsWithSingleQuotes = "'$sectionName'";

  String sql = """
     SELECT COUNT(*) FROM ftp_files WHERE "isUnpacked" is NULL AND section_name IN ($sectionsWithSingleQuotes) AND fz = '${serviceStatus['fz']}'
    """;
  print(sql);
  try {
    List<List<dynamic>> result = await connection.query(sql).timeout(Duration(seconds: 42));
    int count = result[0][0];

    print('Unprocessed Archives Count: $count');
    serviceStatus['currentTask'] = '';
    serviceStatus['unprocessedArchivesCount'] = count;
    return count;

  } catch(e) {
    print("EXCEPTION SELECT COUNT(*) FROM ftp_files: $e");

      if(connection.isClosed) { 
        connection = getConnection();
        await connection.open();
        print("reconnected123");
      }  
      await getCountOfUnprocessedArchives(sectionName);

      throw Exception("Cant SELECT COUNT(*) ");
  }
  

}


Future<int> getCountOfUnprocessedXML(String sectionName) async {
  serviceStatus['currentTask'] = 'getCountOfUnprocessedXML';
  // раньше брали список из дозба и сразу для всех проверяли
  // String sectionsWithSingleQuotes = json.encode(ftpJob['sections']).toString().replaceAll('"',"'").replaceAll('[', '').replaceAll(']', '');

  // теперь по одному, но для целей упрощения оставим старое название переменной той что выше
  String sectionsWithSingleQuotes = "'$sectionName'";

  String sql = """
     SELECT COUNT(*) FROM "xml_files" WHERE parsing_status IS NULL AND exists_status IS NULL AND section_name IN ($sectionsWithSingleQuotes) AND fz = '${serviceStatus['fz']}'
    """;
  print(sql);
  try {
    List<List<dynamic>> result = await connection.query(sql).timeout(Duration(minutes: 1));
     int count = result[0][0];
    print('Unprocessed XML Count: $count');
    serviceStatus['currentTask'] = '';
    serviceStatus['unprocessedXMLCount'] = count;
    return count;
  } on Exception {
    print("Exception in getCountOfUnprocessedXML: ${e}");
    await getCountOfUnprocessedXML(sectionName);
    rethrow; 
  }

}

setArchiveUnpackFlagToTrue(FTPItemModel archiveItem) async {
  print("setArchiveUnpackFlagToTrue");
  serviceStatus['currentTask'] = 'setArchiveUnpackFlagToTrue';
  // если файл обработали, то из пути нужно убрать currMonth/
  String sql = """UPDATE ftp_files SET "isUnpacked" = 'TRUE', ftp_path = '${archiveItem.ftpPath.replaceAll('currMonth/', '')}' WHERE "archive_name"='${archiveItem.archiveName}'""";
  // print(sql);
  await connection.query(sql);
  print('Unpacked: ${archiveItem.archiveName}');
  serviceStatus['currentTask'] = '';
}

// бывают битые архивы
setArchiveUnpackFlagToCorrupted(FTPItemModel archiveItem) async {
  print("setArchiveUnpackFlagToCorrupted");
  serviceStatus['currentTask'] = 'setArchiveUnpackFlagToCorrupted';
  // если файл обработали, то из пути нужно убрать currMonth/
  String sql = """UPDATE ftp_files SET "isUnpacked" = 'CORRUPTED', ftp_path = '${archiveItem.ftpPath.replaceAll('currMonth/', '')}' WHERE "archive_name"='${archiveItem.archiveName}'""";
  // print(sql);
  await connection.query(sql);
  print('Unpacked: ${archiveItem.archiveName}');
  serviceStatus['currentTask'] = '';
}

// setArchiveUnpackFlagToCorrupted

// если архив был очень маленький, то пометим его как карлик
setArchiveUnpackFlagToDwarf(FTPItemModel archiveItem) async {
  serviceStatus['currentTask'] = 'setArchiveUnpackFlagToDwarf';
  // если файл обработали, то из пути нужно убрать currMonth/
  String sql = """UPDATE ftp_files SET "isUnpacked" = 'DWARF', ftp_path = '${archiveItem.ftpPath.replaceAll('currMonth/', '')}' WHERE "archive_name"='${archiveItem.archiveName}'""";
  print(sql);
  await connection.query(sql);
  print('DWARF Archive Ignored: ${archiveItem.archiveName}');
  serviceStatus['currentTask'] = '';
}



// принимаем решение распаковавать архивы или файлов пока достаточно
choiceOfProcessingAction() async {
  print("choiceOfProcessingAction");
  serviceStatus['currentTask'] = 'choiceOfProcessingAction';
  // await checkIfConnectedOrReconnectToFTP();


  // не смотря на то, что мы тут перебираем секции и проверку устраиваем для каждой
  // в самих запросах на выборку у нас данные из всего ftpJob['sections'] идут
  // сделано т.к. возможно дальше задачи для обработки будут в другом формате 
  // не одна глобальная, а отдельные

  for(String sectionName in ftpJob['sections']) { 

    int achivesUnprocessedCount = await getCountOfUnprocessedArchives(sectionName);

      currentJob['achivesUnprocessedCount'] = achivesUnprocessedCount;

      if(achivesUnprocessedCount > 1) { // если у нас есть больше одного архива

        int xmlUnprocessedCount = await getCountOfUnprocessedXML(sectionName); // сколько файлов у нас со статусом parsing_status IS NULL
          // если файлов осталось меньше нормы, то нужно распаковать новые
          // currentJob['filesInserted'] сколько мы уже за один запуск вставили, чтобы не дергать каунт каждый раз
        
        if(xmlUnprocessedCount < ftpJob['atLeastNotProcessedXML']) { // если файлов осталось мало
          while((xmlUnprocessedCount + currentJob['filesInserted']) < ftpJob['atLeastNotProcessedXML']) {
            print("NOT ENOUGH files: ${xmlUnprocessedCount + currentJob['filesInserted']} while needed at least ${ftpJob['atLeastNotProcessedXML']}. Running!");
            await runFtpParsingOrOnlyUnpackArchives();

            // при каждом вызове runFtpParsingOrOnlyUnpackArchives() мы дергаем downloadArchive() внутри нее
            // поэтому декремент наверно в сам downloadArchive пихать не надо. Тут достаточно.
            currentJob['achivesUnprocessedCount']--;

            if(currentJob['achivesUnprocessedCount'] == 0) {
              print("It's seems that all Archives processed");
              break; // если архивов больше нет, то тоже прервем
            } 
          }
        } else {
          print("No need to start Parsing and archives downloading/unpacking. That's enough files: $xmlUnprocessedCount");
          print("That's enough files: $xmlUnprocessedCount while needed at least ${ftpJob['atLeastNotProcessedXML']}");
          serviceStatus['currentTask'] = 'enough xml files $xmlUnprocessedCount ';
        }

      } else {
        print("Nothing to unpack achivesUnprocessedCount: $achivesUnprocessedCount");
        await runFtpParsingOrOnlyUnpackArchives();
      }

  
     serviceStatus['currentTask'] = '';

    
  }// проверку нужно сделать для каждой отдельной секции


 

}

// чтобы слишком часто не дергать парсер FTP мы записываем время его последнего запуска
runFtpParsingOrOnlyUnpackArchives() async {
  print("runFtpParsingOrOnlyUnpackArchives");
  serviceStatus['currentTask'] = 'runFtpParsingOrOnlyUnpackArchives';
  
  if(currentJob['lastRun'].length > 0) { // значит что-то указано
    currentJob['lastRun'] = DateTime.now().toString();
    // если с последнего запуска прошло более часа, то пора ба запуститься снова
    DateTime lastRun = DateTime.parse(currentJob['lastRun']);
    print(lastRun);
    if(lastRun.isBefore(DateTime.now().subtract(Duration(hours: 12)))) { // сканируем FTP не чаще раза в 6 часов
      print("6 hours pass. Running new FTP Scan");
      sleep(const Duration(seconds:6));
      await saveArchivesToDB();  // запускаем парсинг FTP и сохранение новых файлов 
      var result = await downloadArchive();
        if(result != null && result == -1) { // нет архивов для загрузки, нужно выйти
          return;
        }
      // await retry(() async {
      //   },
      // retryIf: (e) => e is SocketException || e is TimeoutException,
      //  );

      
     
    } else {
      // запустим просто загрузку уже спаршенных файлов
      // то что они есть мы уровнем выше в choiceOfProcessingAction уже определили 

      var result = await downloadArchive();
      if(result != null && result == -1) { // нет архивов для загрузки, нужно выйти
        return;
      }

      print("we are here!!!");
      // пока закомментим иначе может быть рекурсия
      if(currentJob['achivesUnprocessedCount'] > 0) {
        await choiceOfProcessingAction();
      }
    }

  } else { // если в currentJob['lastRun'] пусто, значит скорее всего первый запуск
    print("First run of data-loader");
    currentJob['lastRun'] = DateTime.now().toString();
    await saveArchivesToDB(); // парсим файлы при первом запуске
    await downloadArchive(); // загружаем архимы 
    await choiceOfProcessingAction();
    // не нужно вызывать тут выбор действия, иначе в бесконечную рекурсию уйдем если архивов нет
    // await choiceOfProcessingAction(); // снова возвращаемся к выбору действия

  }

  serviceStatus['currentTask'] = '';
  
}


startNewJob(Map<String, dynamic> ftpJob) async {
  print('startNewJob');
  for(var fz in ftpJob['fz']) { // для каждого ФЗ отдельный коннект нужен
    print('Processing fz: $fz');
    serviceStatus['fz'] = fz;
    currentJob['filesInserted'] = 0; // сбросим старое значение
    currentJob['achivesUnprocessedCount'] = 0; // сбросим старое значение
    currentJob['lastRun'] = ''; // сбросим старое значение

    currentlyProcessingFtpFz = fz; // глобальный 
    ftpCredential = selectFTP(fz); // получаем реквизиты для новой работы
    try {
        ftpConnect = getFTPConnection();
        await ftpConnect.connect();

        await choiceOfProcessingAction();
    } on Exception catch(e) {
      await ftpConnect.disconnect();
      print('startNewJob exception');
      print(e);
      startNewJob(ftpJob);
    }
    

  }

  serviceStatus['fz'] = null; 
  serviceStatus['currentTask'] = 'idle';

  // await ftpConnect.disconnect(); // закрываем подключение

}
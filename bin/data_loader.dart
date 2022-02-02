import 'dart:async';

import 'package:blueprint/blueprint.dart';
import 'package:data_loader/Services/ftp_service.dart';
import 'package:data_loader/globals.dart';
import 'package:data_loader/misc.dart';
import 'package:data_loader/validation_schemes.dart';
import 'package:postgres/postgres.dart';
import 'package:alfred/alfred.dart';


const int PORT = 5002;



void main(List<String> arguments) async {
  startupTime = DateTime.now();
  
  loadOnlyOneRegionForTest = false; // сугубо для облегчения тестирования
  
  connection = getConnection();
  await connection.open();  

  // при старте заполним чтобы потом подставлять id нужного региона при вставке XML в БД
  listOfMaps44fz = await getRegionListFromDB('fz44');
  listOfMaps223fz = await getRegionListFromDB('fz223');

  final app = Alfred();
  app.all('*', cors(origin: '*', headers: '*'));
  
  app.get('/', (req, res) async { // тут мы health-check делаем    
    await res.json(healthCheck()); 
  });  


   ftpJob = { // он нужен в глобал
    "fz": ["fz44"],
    "sections": ["notifications", "contracts", "protocols"],
    "start_year": 2015,
    "onlyLastMonth" : false,
    "atLeastNotProcessedXML": 100000, // если не распакованных файлов меньше этого количества, то распаковать новые
  };

   Timer.periodic(
     Duration(hours: 2), (Timer t) async {
       await startNewJob(ftpJob);
     } 
   );
  
  // технически у нас есть возможность запускать работы обращаясь по данному хэндлеру, но на практике это мало нужно
  app.post('/start-job', (req, res) async { // работы стартуем   
    final body = await req.body;
    if(body != null) {
      var isMatched = matchMap(body as Map<String, dynamic>, ftpJob_schema);
      if(!isMatched) {
        print('Wrong json for starting Job: $body');
        await res.json({'error': 'validation error: incorrect input job json' });
      } else {
        ftpJob = body; // сохраним в глобальный объект словарь работы
        await res.json({'status': 'job accepted'}); // сразу дадим ответ
        await startNewJob(body); // сюда предаем прилетевшую работу
      }
    } else {
      await res.json({'error': 'empty request'});
    }

  });    

  
  await app.listen(PORT);


  //  String file = r'D:/zak_data/notifications/Adygeja_Resp/notification_Adygeja_Resp_2019010100_2019020100_001/fcsNotificationEP44_0176100001319000002_19011259.xml';
  //  String file = r'D:\zak_data\notifications\Adygeja_Resp\notification_Adygeja_Resp_2019010100_2019020100_001\fcsNotificationEA615_207600000011900001_19000863.xml';
  // simpleFieldsExtractor(file);


  // getListOfArchives();
  // saveArchivesToDB();
  // getSingleNotUnpackedArchiveFromDB();
  // downloadArchive();
  // getCountOfUnprocessedArchives();

  // simpleFieldsExtractor();

    

}


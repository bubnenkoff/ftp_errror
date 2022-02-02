import 'package:blueprint/blueprint.dart';

// ignore_for_file: non_constant_identifier_names

// Map ftpJob = {
// 	"fz": ["fz44", "fz223"],
// 	"sections": ["notifications", "contracts", "protocols"],
// 	"start_year": 2015,
//  "onlyLastMonth" : false,
//  "atLeastNotProcessedXML": 57000, // если не распакованных файлов меньше этого количества, то распаковать новые
// };


final ftpJob_schema = Map.of({
  'fz': 	ListF,
  'sections': 	ListF,
  'start_year': 	IntF,
  'onlyLastMonth': 	BoolF,
  'atLeastNotProcessedXML': 	IntF,
});


// Map currentJob = {
//   "filesInserted": 0, // сколько всего мы за один запуск парсера файлов вставили
//   "achivesUnprocessedCount": 0,
//   "lastRun": "" // дата последнего запуска парсера
// };


Map currentJob_schema = Map.of({
  "filesInserted": IntF, // сколько всего мы за один запуск парсера файлов вставили
  "achivesUnprocessedCount": IntF,
  "lastRun": StringF // дата последнего запуска парсера
});
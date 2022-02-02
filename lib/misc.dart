import 'dart:convert';
import 'dart:io';
import 'package:data_loader/Models/regions_model.dart';
import 'package:data_loader/globals.dart';
import 'package:xml/xml.dart';
import 'package:xml/xml_events.dart';
import 'package:xpath_selector/xpath_selector.dart';

  String? extractValue(String fileContent, List<String> variantList) {

  // final file = File(file_name).readAsStringSync();
  // final selector = XPath.xml(file).query('//*[local-name()="purchaseNumber"]');
  // print(selector.node!.text);


    // final document = XmlDocument.parse(fileContent);
    String? result;
      for(var el in variantList) {        
        final elements = XPath.xml(fileContent).query('//*[local-name()="$el"]');
        if(elements.nodes.isNotEmpty) {
          result = elements.nodes.first.text;
          break;
        }
      }

    return result; 
  }


  // Future<String?> extractValue(File file, List<String> variantList) async  {
  //   String? result;
  //   // final file = File(file_name);
  //     for(var el in variantList) {  
  //         await file.openRead()
  //         .transform(utf8.decoder)
  //         .toXmlEvents()
  //         .selectSubtreeEvents((event) => variantList.contains(event.localName))
  //         .toXmlNodes()
  //         .forEach((node) {
  //            result = node[0].innerText;
  //         } );

  //         return result;
  //     }

  //   return result; 
  // }


healthCheck() {
   serviceStatus['uptime'] = DateTime.now().difference(startupTime).toString();
   serviceStatus['isDbConnected'] = !connection.isClosed; // реверсируем чтобы логика была прямая

   return serviceStatus; 
}  


Future<List<Map<dynamic, dynamic>>> getRegionListFromDB(String fzName) async {
  if(fzName == "fz44") {
    String sql = "SELECT id, ru_name, fz44_name  FROM regions;";
    List<List<dynamic>> result = await connection.query(sql).timeout(Duration(seconds: 1));  
    var data = RegionsModel.listFromArrays(result); 
    return data;

  }

  if(fzName == "fz223") {
    String sql = "SELECT id, ru_name, fz223_name  FROM regions;";
    List<List<dynamic>> result = await connection.query(sql).timeout(Duration(seconds: 1));  
    var data = RegionsModel.listFromArrays(result);     
    return data;
  }

  throw Exception("You are trying to do select in unknown FZ: $fzName");


}
function transform(line){
    var values = line.split(',');
    var obj = new Object();
    obj.Index = values[0];
    obj.Date = values[1];
    obj.Close = values[2];
    obj.High = values[3];
    obj.Low = values[4];
    obj.Open = values[5];
    obj.Volume = values[6];
    var jsonString = JSON.stringify(obj);
    return jsonString;
}
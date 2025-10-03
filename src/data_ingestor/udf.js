function transform(line){
    var values = line.split(',');
    var obj = new Object();
    obj.Date = values[0];
    obj.Close = values[1];
    obj.High = values[2];
    obj.Low = values[3];
    obj.Open = values[4];
    obj.Volume = values[5];
    var jsonString = JSON.stringify(obj);
    return jsonString;
}
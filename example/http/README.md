# curl sample
curl --insecure -H "Content-Type: application/json; charset=utf-8" \
-X POST \
-d '{"dataHeader":{"rowCnt":1}, "dataBody":{"cart_div":"1", "lbl":"FridayNight!"}}' \
http://127.0.0.1:8888/longjob
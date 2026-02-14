## Регионы

GET https://www.sberbank.ru/proxy/services/dict-region/region/long

- Москва:  
  код: 77  
  код Сбера: 038
- Краснодарский край:  
  код: 23  
  код Сбера: 052


## Виды металлов и котировок

GET https://www.sberbank.ru/proxy/services/rates/public/v2/crtList?baseTypes[]=PMR-1&extraTypes[]=PMR-3&...&cmType=METAL

Металлы:
- `A99` - Серебро
- `A98` - Золото

Типы курсов:
- `PMR-1` - Слитки
- `PMR-2` - (не существует)
- `PMR-3` - ОМС, Сбербанк Онлайн
- `PMR-4` - ОМС, Сбер Премьер
- `PMR-5` - ОМС, Сбербанк Золотой (отключено)
- `PMR-6` - ОМС, Сбер Первый
- `PMR-7` - ОМС, Private Banking

ОМС - "Обезличенные металлические счета в ВСП и УКО"


## Котировки металлов

Для слитков:  
GET https://www.sberbank.ru/proxy/services/rates/public/v2/historyIngots?rateType=PMR-1&isoCode=A99&date=1769461200000&segType=TRADITIONAL&id=38

Для ОМС:  
GET https://www.sberbank.ru/proxy/services/rates/public/v3/branchHistory?rateType=PMR-3&isoCode=A99&date=1770584400000&id=38
GET https://www.sberbank.ru/proxy/services/rates/public/v3/history?rateType=PMR-3&isoCode=A99&date=1769461200000

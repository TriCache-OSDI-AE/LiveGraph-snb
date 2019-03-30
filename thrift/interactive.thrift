namespace cpp interactive
namespace d interactive
namespace java interactive
namespace php interactive
namespace perl interactive

struct ShortQuery1Request {
    1: i64 personID,
}

struct ShortQuery1Response {
    1: string firstName,
    2: string lastName,
    3: i64 birthday,
    4: string locationIp,
    5: string browserUsed,
    6: i64 cityId,
    7: string gender,
    8: i64 creationDate,
}

service Interactive {
    ShortQuery1Response shortQuery1(1:ShortQuery1Request request),
}

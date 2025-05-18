from neverraise import Err, Ok, Result


def divide(a: int, b: int) -> Result[float, str]:
    if b == 0:
        return Err("Division by zero")
    return Ok(a / b)


match divide(10, 0):
    case Ok(result):
        print(result)
    case Err(error):
        print(error)


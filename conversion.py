def ftintom(ft, inch = 0.0):
    return ft * 0.3048 + inch * 0.0254


def lbstokg(lb):
    return lb * 0.45359237


def bmi(weight, height):
    if type(weight) != float and type(weight) != int or\
    type(height) != float and type(height) != int:
        return None
    elif height < 0.5 or height > 3.0 or \
    weight < 2 or weight > 650 :
        return None

    return weight / height ** 2


print(bmi(weight = lbstokg(176), height = ftintom(5, 7)))

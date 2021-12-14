from Tram import Tram
from datetime import datetime


def main():
    t = Tram(25, 21.033324, "1185+1184", datetime.strptime("2021-10-07 14:30:14", '%Y-%m-%d %H:%M:%S'), 52.254287,
             "10")

    print(t)
    print(t.find_stop())

if __name__ == '__main__':
    main()

#Magda - tutaj bylam
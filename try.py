def DisplayBoard(board):
#
# the function accepts one parameter containing the board's current status
# and prints it out to the console
#

    for i in range(len(board)):
        str=lin=hed=''
        for j in range(len(board[i])):
            hed = hed  + '+-------'
            lin = lin  + '|       '
            str = str  + '|   ' + board[i][j]+'   '
        hed=hed+'+'
        lin=lin+'|'
        print(hed)
        print(lin)
        str=str + '|'
        print(str)
        print(lin)
    print(hed)
    return True

def EnterMove(board):
#
# the function accepts the board current status, asks the user about their move,
# checks the input and updates the board according to the user's decision
#

    while True:
        try:
            print("Enter '-999' to exit")
            inp=int(input("Enter a Move: "))
            if inp == -999:
                break
            elif inp < 0  or inp >9:
                print("Not a Valid Number")
            elif board[(inp -1)  // 3][(inp - 1) % 3] != str(inp):
                print("The cell is already occupied.Renter")
            else:
                break
        except:
            print("What you entered is not an integer. ReEnter")

    return inp

def MakeListOfFreeFields(board):
#
# the function browses the board and builds a list of all the free squares;
# the list consists of tuples, while each tuple is a pair of row and column numbers
#

    return

board = [['O','O','X'],['4','5','6'],['O','8','X']]

inp=EnterMove(board)
print(DisplayBoard(board))

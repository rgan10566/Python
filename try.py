from random import randrange

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

# #Ramesh's version. has a great mathematical logic to find if in the list a field is empty
# def EnterMove(board):
# #
# # the function accepts the board current status, asks the user about their move,
# # checks the input and updates the board according to the user's decision
# #
#
#     while True:
#         try:
#             print("Enter '-999' to exit")
#             inp=int(input("Enter a Move: "))
#             if inp == -999:
#                 break
#             elif inp < 0  or inp >9:
#                 print("Not a Valid Number")
#             elif board[(inp -1)  // 3][(inp - 1) % 3] != str(inp):
#                 print("The cell is already occupied.Renter")
#             else:
#                 break
#         except:
#             print("What you entered is not an integer. ReEnter")
#
#     return inp

def EnterMove(board):
#
# the function accepts the board current status, asks the user about their move,
# checks the input and updates the board according to the user's decision
#
    Valid = False
    try:

        inp=int(input("Enter a Move: "))
        if inp < 0  or inp >9:
            print("Not a Valid Number, ReEnter")
            Valid = False
        elif tuple(str((inp - 1) // 3)+str((inp - 1) % 3)) in  freelist:
#            print("The cell "+str(inp)+" is updated ")
            board[(inp - 1) // 3][(inp - 1) % 3]='X'
            Valid = True
        else:
            print("you probably entered a field that is already filled, Please ReEnter")
            Valid = False
    except:
        print("What you entered is not an integer. ReEnter")
        Valid = False

    return Valid



def MakeListOfFreeFields(board):
#
# the function browses the board and builds a list of all the free squares;
# the list consists of tuples, while each tuple is a pair of row and column numbers
#
    freelist=[]
    for i in range(len(board)):
        for j in range(len(board[i])):
            # this one uses a list and not a string
            # templist=[]
            # templist.append(i)
            # templist.append(j)
            tempstr=str(i)+str(j)
            if board[i][j] != 'O' and board[i][j] != 'X' :
                freelist.append(tuple(tempstr))
    return freelist

def VictoryFor(board, sign):
#
# the function analyzes the board status in order to check if
# the player using 'O's or 'X's has won the game
#
    rVictory = [ True, True, True]
    cVictory = [True, True, True]
    dVictory = [True,True]
    for i in range(len(board)):
        for j in range(len(board[i])):
            if board[i][j] == sign:
                if rVictory[i]: rVictory[i] = True
                if cVictory[j]: cVictory[j] = True
                if i == j and dVictory[0]:
                    dVictory[0] = True
                elif ((i == 0 and j == 2) or (i == 1 and j == 1) or (i == 2 and j == 0)) and dVictory[1]:
                    dVictory[1] = True
            else:
                rVictory[i] = False
                cVictory[j] = False
                if i == j:
                    dVictory[0] = False
                elif ((i == 0 and j == 2) or (i == 1 and j == 1) or (i == 2 and j == 0)):
                    dVictory[1] = False

    if True in dVictory:
        Victory = True
    elif True in rVictory:
        Victory = True
    elif True in cVictory:
        Victory = True
    else:
        Victory = False

    return Victory


def DrawMove(board):
#
# the function draws the computer's move and updates the board
#
    Valid = False
    while True:
        compmove = randrange(10)
        freelist = MakeListOfFreeFields(board)
        if tuple(str((compmove - 1) // 3)+str((compmove - 1) % 3)) in freelist:
            board[(compmove - 1) // 3][(compmove - 1) % 3]='O'
            Valid = True
            break

    return Valid

board = [['1','2','3'],['4','5','6'],['7','8','9']]

#for practice - remove later
#board = [['O','O','X'],['O','X','O'],['O','O','O']]
#VictoryFor(board, 'O')
print("Initial Board")
DisplayBoard(board)
freelist=[(0,0),(0,1),(0,2),(1,0),(1,1),(1,2),(2,0),(2,1),(2,2)]
print("Computer makes the first Move")
switch = True

while True:
    freelist=MakeListOfFreeFields(board)
    if freelist == []:
        print("The Game is completed")
        if VictoryFor(board,'O'):
            print("Computer Won")
        elif VictoryFor(board,'X'):
            print("You Won")
        else:
            print("It was a draw")
        break

    else:
        if switch:
            print("Computer's Move")
            Valid = DrawMove(board)
            if Valid : switch=False
        else:
            print("Your Move")
            Valid=EnterMove(board)
            if Valid: switch=True

        if VictoryFor(board,'O'):
            print("Computer Won")
            break
        elif VictoryFor(board,'X'):
            print("You Won")
            break

        DisplayBoard(board)

DisplayBoard(board)

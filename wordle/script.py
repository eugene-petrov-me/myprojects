## https://realpython.com/python-wordle-clone/

import random

MAX_GUESSES = 6     #a variable to determine the number of guesses

def main():
    # Pre-process
    secret_word = get_random_word('word_list.txt')

    # Process (main loop)
    for guess_num in range(0, MAX_GUESSES):     #start a loop for the number of guesses

        guess = input(f'\nGuess {guess_num + 1}: ').upper()
        while True:
            if len(guess) != 5:
                guess = input('The guess should be a 5 letter word! \nTry again: ').upper()
            else: 
                break

        show_guess(guess, secret_word)
        if guess == secret_word:
            break

    # Post-process
    else:
        game_over(secret_word)

def get_random_word(filename):

    word_list = []      #create an empty list for storing words

    #read a file and append into the list
    with open(filename, 'r') as file:
        word_list = [
            word.replace('\n', '').upper() 
            for word in file 
            if len(word.replace('\n', '')) == 5
            ]
    
    return random.choice(word_list)      #randomly select a word from the list

def show_guess(guess, secret_word):

    correct_letters = {letter for letter, correct in zip(guess, secret_word) if letter == correct}
    misplaced_letters = set(guess) & set(secret_word) - correct_letters
    wrong_letters = set(guess) - set(secret_word)

    print('Correct letters:', ', '.join(sorted(correct_letters)))
    print('Misplaced letters:', ', '.join(sorted(misplaced_letters)))
    print('Wrong letters:', ', '.join(sorted(wrong_letters)))

def game_over(secret_word):
    print(f'The word was {secret_word}')

if __name__=='__main__':
    main()
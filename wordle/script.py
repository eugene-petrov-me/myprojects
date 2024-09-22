## https://realpython.com/python-wordle-clone/

import random
from rich.console import Console
from rich.theme import Theme

console = Console()

MAX_GUESSES = 6     #set a variable to determine the number of guesses

def main():
    # Pre-process
    secret_word = get_random_word('word_list.txt')
    user_guesses = ["_" * 5] * MAX_GUESSES
    
    # Process (main loop)
    for guess_num in range(0, MAX_GUESSES):     #start a loop for the number of guesses
        
        refresh_screen(f'Guess #{guess_num + 1}')
        show_guesses(user_guesses, secret_word)

        user_guesses[guess_num] = input('\nYour Guess: ').upper()
        while True:
            if len(user_guesses[guess_num]) != 5:
                user_guesses[guess_num] = input('The guess should be a 5 letter word! \nTry again: ').upper()
            else: 
                break

        if user_guesses[guess_num] == secret_word:
            break

    # Post-process
    else:
        show_guesses(user_guesses, secret_word)
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

def show_guesses(guesses, secret_word):
    for guess in guesses:
        styled_guess = []
        for letter, correct in zip(guess, secret_word):
            if letter == correct:
                style = 'bold white on green'
            elif letter in secret_word:
                style = 'bold white on yellow"'
            else: 
                style = 'white on #666666' 
            styled_guess.append(f'[{style}]{letter}[/]')
        console.print(''.join(styled_guess))

def refresh_screen(headline):
    console.clear()
    console.rule(f"[bold blue]:leafy_green: {headline} :leafy_green:[/]\n")

def game_over(secret_word):
    print(f'The word was {secret_word}')

if __name__=='__main__':
    main()
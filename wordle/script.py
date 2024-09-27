## https://realpython.com/python-wordle-clone/

import random
import re
import contextlib
from rich.console import Console
from rich.theme import Theme
from string import ascii_uppercase

console = Console()

MAX_GUESSES = 6     #set a variable to determine the number of guesses
WORD_LENGTH = 5     #set a variable to determine the length of the word

def main():
    # Pre-process
    secret_word = get_random_word('word_list.txt')
    user_guesses = ["_" * WORD_LENGTH] * MAX_GUESSES

    # Process (main loop)
    with contextlib.suppress(KeyboardInterrupt):
        for guess_num in range(0, MAX_GUESSES):     #start a loop for the number of guesses
            
            refresh_screen(f'Guess #{guess_num + 1}')
            show_guesses(user_guesses, secret_word)

            guess = input('\nYour Guess: ').upper()
            while True:
                if len(guess) != 5:
                    guess = input('The guess should be a 5 letter word! \nTry again: ').upper()
                elif guess in user_guesses:
                    print(f'You already guessed {guess}')
                    guess = input('Try again:').upper()
                elif re.compile('[^A-Z]').search(guess):
                    guess = input('Word must contain only letters! \nTry again: ').upper()
                else: 
                    break

            user_guesses[guess_num] = guess

            if user_guesses[guess_num] == secret_word:
                show_guesses(user_guesses, secret_word)
                print('You guessed it!')
                break

    # Post-process
    game_over(user_guesses, secret_word, guessed_correctly=user_guesses[guess_num] == secret_word)

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

def show_guesses(guesses, secret_word):
    styled_alphabet = {letter:letter for letter in ascii_uppercase}
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
            if letter != "_":
                styled_alphabet[letter] = f'[{style}]{letter}[/]'
        console.print(''.join(styled_guess), justify="center")
    console.print("\n" + ''.join(styled_alphabet.values()), justify="center")

def refresh_screen(headline):
    console.clear()
    console.rule(f"[bold blue]:leafy_green: {headline} :leafy_green:[/]\n")

def game_over(user_guesses, secret_word, guessed_correctly):
    show_guesses(user_guesses, secret_word)
    if guessed_correctly:
        console.print(f"\n[bold white on green]Correct, the word is {secret_word}[/]")
    else:
        console.print(f"\n[bold white on red]Sorry, the word was {secret_word}[/]")

if __name__=='__main__':
    main()
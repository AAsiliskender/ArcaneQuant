
def main():
    # To initialise here
    print('Initialising ArcaneQuant:') # TODO: GIVE VERSION, ADD GUI AND CHECK IF I NEED TO IMPORT
    import arcanequant.quantlib as ql
    print('Importing QuantLib as \'ql\', use this alias for operations.')
    # Importing tools here do not carry over to later classes


    print("Enter Python (3.12+) code to execute. Type 'quit' to exit.")
    from arcanequant.fend import backendInterfacer, app # Launch app
    return # Don't run CLI (for now)

    while True:
        user_input = input(">>> ")
        
        if user_input.strip().lower() in {"quit", "exit"}:
            print("Exiting.")
            break

        try:
            # Try evaluating expressions (e.g., 1 + 1, len("abc"))
            result = eval(user_input)
            if result is not None:
                print(result)
        except SyntaxError:
            # Fallback to exec for statements (e.g., def, for, etc.)
            try:
                exec(user_input)
            except Exception as e:
                print(f"Error: {e}")
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()


/**
 * @copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

/* This code takes in list of file names in argv and embed the contents
 * into a const char array.
 **/

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    FILE *fp;
    int i, j, ch;

    printf("extern const unsigned char jsFunction_src[] = {");
    for(i = 1; i < argc; i++) {
        if ((fp = fopen(argv[i], "rb")) == NULL) {
            exit(EXIT_FAILURE);
        }
        else {
            for (j = 0; (ch = fgetc(fp)) != EOF; j++) {
                if ((j % 12) == 0) {
                    printf("%c", '\n');
                }
                printf(" %#02x,", ch);
            }
            fclose(fp);
        }
    }

    // Append zero byte at the end, to make text files appear in memory
    // as nul-terminated strings.
    printf("%s", " 0x00\n};\n");

    return EXIT_SUCCESS;
}

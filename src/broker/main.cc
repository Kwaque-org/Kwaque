#include "src/broker/application.h"

int main(int argc, char** argv) {
    kwaque::broker::application app;
    return app.run(argc, argv);
}
